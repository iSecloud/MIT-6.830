package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;

import javax.swing.ToolTipManager;

import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pageId;
    final TupleDesc tupleDesc;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;
    
    /**
     * dirtyId: 标记脏数据的事务
     * isDirty: 是否是脏页
     */
    private TransactionId dirtyTid;
    private boolean isDirty; 

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pageId = id;
        this.tupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        int tupleSize = tupleDesc.getSize();
        int tuplesNum = (BufferPool.getPageSize() * 8) / (tupleSize * 8 + 1);
        return tuplesNum;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        int tuplesNum = getNumTuples();
        int headerSize = (int)Math.ceil(Double.valueOf(tuplesNum) / 8);
        return headerSize;
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pageId,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        	oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
	    return pageId;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<tupleDesc.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(tupleDesc);
        RecordId rid = new RecordId(pageId, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<tupleDesc.numFields(); j++) {
                Field f = tupleDesc.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<tupleDesc.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<tupleDesc.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + tupleDesc.getSize() * tuples.length); //- numSlots * tupleDesc.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple tup) throws DbException {
    	if (!tup.getRecordId().getPageId().equals(this.pageId)) {
    		throw new DbException("该元组" + tup.toString() + "不存在与该页面中:" + this.pageId.toString());
    	}
        int tIndex = tup.getRecordId().getTupleNumber();
        if (!isSlotUsed(tIndex)) {
        	throw new DbException("该元组" + tup.toString() + "已经被删除了");
        }
        markSlotUsed(tIndex, false);
        tuples[tIndex] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple tup) throws DbException {
        if (!tup.getTupleDesc().equals(this.tupleDesc)) {
        	throw new DbException("元组属性不匹配，无法插入");
        }
        if (getNumEmptySlots() == 0) {
        	throw new DbException("该page没有额外空间，无法插入");
        }
        for (int slot = 0; slot < numSlots; slot += 1) {
        	if (!isSlotUsed(slot)) {
        		tup.setRecordId(new RecordId(pageId, slot));
        		markSlotUsed(slot, true);
        		tuples[slot] = tup;
        		break;
        	}
        }
    }	

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirtyTid = tid;
        this.isDirty = dirty;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return isDirty ? dirtyTid : null; 
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int emptySlots = 0;
        for (int tIndex = 0; tIndex < tuples.length; tIndex ++) {
        	if (!isSlotUsed(tIndex)) {
        		emptySlots ++;
        	}
        }
        return emptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int tIndex) {
    	int valid = (header[tIndex / 8] >> (tIndex % 8)) & 1;
        return !(valid == 0);
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int tIndex, boolean value) {
        if (isSlotUsed(tIndex) == value) {
        	return;
        }
        byte byteVal = (byte) (1 << (tIndex % 8));
        header[tIndex / 8] = (byte) (byteVal ^ header[tIndex / 8]);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
    	ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
    	for (int tIndex = 0; tIndex < tuples.length; tIndex ++) {
    		if (isSlotUsed(tIndex)) {
    			tupleList.add(tuples[tIndex]);
    		}
    	}
    	return tupleList.iterator();
    }

}

