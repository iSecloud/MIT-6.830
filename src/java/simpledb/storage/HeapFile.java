package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.optimizer.IntHistogram;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.random.RandomGeneratorFactory;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param file
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	private File heapFile;
	private TupleDesc tupleDesc;
	
    public HeapFile(File file, TupleDesc td) {
        heapFile = file;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
    	return heapFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile. (tableId)
     */
    public int getId() {
    	return heapFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try (RandomAccessFile rf = new RandomAccessFile(heapFile, "r")) {
			int pageNumber = pid.getPageNumber();
			int pageSize = BufferPool.getPageSize();
			HeapPageId hpid = new HeapPageId(pid.getTableId(), pid.getPageNumber());
			byte[] data = new byte[pageSize];
			rf.seek(pageNumber * pageSize);
			rf.read(data, 0, pageSize);
			return new HeapPage(hpid, data);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        try (RandomAccessFile rf = new RandomAccessFile(heapFile, "rw")) {
        	int pageNumber = page.getId().getPageNumber();
        	int pageSize = BufferPool.getPageSize();
        	byte[] data = page.getPageData();
        	rf.seek(pageNumber * pageSize);
        	rf.write(data, 0, pageSize);
        } catch (IOException e) {
        	throw new IllegalArgumentException(e);
		}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)(heapFile.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple tup)
            throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> pageList = new ArrayList<Page>();
    	// ??????????????????????slot????????
    	for (int pageIndex = 0; pageIndex < numPages(); pageIndex += 1) {
    		// ????????????????????
    		HeapPageId heapPageId = new HeapPageId(getId(), pageIndex);
    		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
    		if (page.getNumEmptySlots() != 0) {
    			// ????????lost????????????????????????????????????
    			// Database.getBufferPool().unsafeReleasePage(tid, heapPageId);
    			page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
    			page.insertTuple(tup);
    			pageList.add(page);
    			return pageList;
    		}
    		else {
    			// ??????????????????????????????
    			if (Database.getBufferPool().getLockManager().pageToPermMap.get(heapPageId).equals(Permissions.READ_ONLY)) {
    				Database.getBufferPool().unsafeReleasePage(tid, heapPageId);
    			}
    		}
    	}
    	// ????????????????????????BufferPool??(????????????HeapFile????????)
    	BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(heapFile, true));
    	bos.write(HeapPage.createEmptyPageData());
    	bos.close();
    	HeapPage page = (HeapPage) Database.getBufferPool().getPage(
    			tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
    	page.insertTuple(tup);
    	pageList.add(page);
    	return pageList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple tup) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> pageList = new ArrayList<Page>();
    	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, tup.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(tup);
        pageList.add(page);
        return pageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }
    
    public class HeapFileIterator implements DbFileIterator {
    	/**
    	 * tid: ????id
    	 * pageNumber: ??????????????
    	 * tpIterator: ??????????????????
    	 * headFile: ??????
    	 */
    	private TransactionId tid;
    	private int pageNumber;
    	private Iterator<Tuple> tpIterator;
    	private HeapFile heapFile;
    	
    	public HeapFileIterator(HeapFile heapfile, TransactionId tid) {
    		this.heapFile = heapfile;
    		this.tid = tid;
    	}
    	
    	/**
    	 * ????????tuple??????
    	 * @throws DbException 
    	 * @throws TransactionAbortedException 
    	 */
    	public Iterator<Tuple> getTupleIterator() throws TransactionAbortedException, DbException {
    		if (pageNumber < 0 || pageNumber >= heapFile.numPages()) {
    			throw new DbException(String.format("The pageNumber is %d not exist", pageNumber));
    		}
    		HeapPageId pageId = new HeapPageId(heapFile.getId(), pageNumber);
			HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
			return page.iterator();
    	}
    	
    	/**
         * Opens the iterator
         * @throws DbException when there are problems opening/accessing the database.
         */
		@Override
		public void open() throws DbException, TransactionAbortedException {
			pageNumber = 0;
			tpIterator = getTupleIterator();
		}
		
		/** @return true if there are more tuples available, 
		 * false if no more tuples or iterator isn't open. 
		 */
		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			if (tpIterator == null) {
				return false;
			}
			if (tpIterator.hasNext()) {
				return true;
			}
			
			if (pageNumber == heapFile.numPages() - 1) {
				return false;
			}
			
			pageNumber ++;
			tpIterator = getTupleIterator();
			return hasNext();
		}

		@Override
		/**
	     * Gets the next tuple from the operator (typically implementing by reading
	     * from a child operator or an access method).
	     *
	     * @return The next tuple in the iterator.
	     * @throws NoSuchElementException if there are no more tuples
	     */
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			return tpIterator.next();
			
		}

		@Override
		/**
	     * Resets the iterator to the start.
	     * @throws DbException When rewind is unsupported.
	     */
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
		}

		@Override
		/**
	     * Closes the iterator.
	     */
		public void close() {
			tpIterator = null;
		}
    }

}