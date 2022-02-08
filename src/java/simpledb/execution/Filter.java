package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

import org.junit.runners.Parameterized.UseParametersRunnerFactory;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    
    /**
     * predicate: 过滤条件
     * child: 传入的迭代器，用于获取tuple
     * td: 表描述
     * childTupsList: 过滤后的tupleList
     * iterator: tupleList迭代器
     */
    private Predicate predicate;
    private OpIterator child;
    private TupleDesc td;
    private List<Tuple> childTupsList = new ArrayList<Tuple>();
    private Iterator<Tuple> iterator;
    
    public Filter(Predicate p, OpIterator child) {
    	this.predicate = p;
    	this.child = child;
    	this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        while (child.hasNext()) {
        	Tuple tup = child.next();
        	if (predicate.filter(tup)) {
        		childTupsList.add(tup);
        	}
        }
        iterator = childTupsList.iterator();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
        iterator = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        iterator = childTupsList.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
    	if (iterator != null && iterator.hasNext()) {
            return iterator.next();
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
    	return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	this.child = children[0];
    }

}
