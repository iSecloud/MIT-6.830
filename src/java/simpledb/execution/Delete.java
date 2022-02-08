package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

import javax.swing.text.StyledEditorKit.BoldAction;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    
    private TransactionId tid;
    private OpIterator child;
    private TupleDesc td;
    private boolean isCalled;
    
    public Delete(TransactionId tid, OpIterator child) {
       this.tid = tid;
       this.child = child;
       this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
       this.isCalled = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	isCalled = false;
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	isCalled = false;
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        int deleteCount = 0;
    	if (isCalled == true) {
        	return null;
        }
    	isCalled = true;
    	while(child.hasNext()) {
    		Tuple tuple = child.next();
        	try {
    			Database.getBufferPool().deleteTuple(tid, tuple);
    			deleteCount += 1;
    		} catch (DbException | IOException | TransactionAbortedException e) {
    			// TODO ºöÂÔÉ¾³ýÊ§°ÜµÄorÖ±½Ó±¨´í£¿
    			continue;
    		}
    	}
    	Tuple resulTuple = new Tuple(td);
    	resulTuple.setField(0, new IntField(deleteCount));
    	return resulTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
