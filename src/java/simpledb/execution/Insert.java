package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private TupleDesc td;
    private boolean isCalled;
    
    public Insert(TransactionId tid, OpIterator child, int tableId)
            throws DbException {
        this.tid = tid;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
        this.isCalled = false;
    }
    
    public TupleDesc getTupleDesc() {
    	return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        isCalled = false;
        super.open();
    }

    public void close() {
    	super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        isCalled = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int insertCount = 0;
    	if (isCalled == true) {
    		return null;
    	}
    	isCalled = true;
    	while(child.hasNext()) {
    		Tuple tuple = child.next();
        	try {
    			Database.getBufferPool().insertTuple(tid, tableId, tuple);
    			insertCount += 1;
    		} catch (DbException | IOException e) {
    			e.printStackTrace();
    			break;
    		}
    	}
    	Tuple resulTuple = new Tuple(td);
    	resulTuple.setField(0, new IntField(insertCount));
    	return resulTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
       child = children[0];
    }
}
