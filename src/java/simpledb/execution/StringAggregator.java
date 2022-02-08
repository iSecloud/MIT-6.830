package simpledb.execution;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import javax.lang.model.element.NestingKind;
import javax.swing.GroupLayout.Group;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op op;
    private ConcurrentHashMap<Field, Integer> aggregateMap;
    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.aggregateMap = new ConcurrentHashMap<Field, Integer>();
        if (!this.op.equals(Op.COUNT)) {
        	throw new IllegalArgumentException("聚合操作不支持");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (tup.getField(afield) != null) {
    		Field gbField = gbfield == Aggregator.NO_GROUPING ? Aggregator.NO_GROUP_FIELD : tup.getField(gbfield);
        	int count = aggregateMap.containsKey(gbField) ? aggregateMap.get(gbField) + 1 : 1;
        	aggregateMap.put(gbField, count);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
    	return new StringAggrOpItertor(aggregateMap, gbfieldType);
    }
    
    class StringAggrOpItertor implements OpIterator {
    	
    	private ConcurrentHashMap<Field, Integer> aggregateMap;
    	private Type gbfieldType;
    	private TupleDesc td;
    	private Iterator<Map.Entry<Field, Integer>> mapIterator;
    	
    	public StringAggrOpItertor(ConcurrentHashMap<Field, Integer> aMap, Type gbfieldType) {
    		this.aggregateMap = aMap;
    		this.gbfieldType = gbfieldType;
    		this.mapIterator = null;
    		Type[] typeAr = this.gbfieldType == null ? new Type[] {Type.INT_TYPE} : new Type[] {this.gbfieldType, Type.INT_TYPE};
    		String[] fieldAr = this.gbfieldType == null ? new String[] {"aggregateVal"} : new String[] {"groupVal", "aggregateVal"};
    		this.td = new TupleDesc(typeAr, fieldAr);
    	}
    	
		@Override
		public void open() throws DbException, TransactionAbortedException {
			this.mapIterator = aggregateMap.entrySet().iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			return mapIterator.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			Map.Entry<Field, Integer> item = mapIterator.next();
			Field gbField = item.getKey();
			int aggregateVal = item.getValue();
			Tuple tuple = new Tuple(td);
			if (gbField.equals(Aggregator.NO_GROUP_FIELD)) {
				tuple.setField(0, new IntField(aggregateVal));
			} else {
				tuple.setField(0, gbField);
				tuple.setField(1, new IntField(aggregateVal));
			}
			return tuple;
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			mapIterator = aggregateMap.entrySet().iterator();
		}

		@Override
		public TupleDesc getTupleDesc() {
			return td;
		}

		@Override
		public void close() {
			mapIterator = null;
		}
    }

}
