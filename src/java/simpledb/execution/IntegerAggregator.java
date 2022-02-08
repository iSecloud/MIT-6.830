package simpledb.execution;

import java.nio.channels.NonReadableChannelException;
import java.security.KeyStore.Entry;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import javax.lang.model.element.Element;
import javax.print.attribute.standard.Fidelity;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.optimizer.IntHistogram;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op op;
    private ConcurrentHashMap<Field, Integer> aggregateMap;
    private ConcurrentHashMap<Field, Integer> countMap;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.aggregateMap = new ConcurrentHashMap<Field, Integer>();
        this.countMap = new ConcurrentHashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (tup.getField(afield) == null) {
        	return;
        }
        Field gbField = gbfield == Aggregator.NO_GROUPING ? Aggregator.NO_GROUP_FIELD : tup.getField(gbfield);
        int count = countMap.containsKey(gbField) ? countMap.get(gbField) + 1 : 1;
        int value = aggregateMap.containsKey(gbField) ? aggregateMap.get(gbField) : 0;
        if (!aggregateMap.containsKey(gbField) && op.equals(Op.MIN)) {
        	value = (1 << 31) - 1;
        }
        int afieldVal = ((IntField) tup.getField(afield)).getValue();
        
        if (op.equals(Op.SUM) || op.equals(Op.AVG)) {
        	value = value + afieldVal;
        } 
        else if(op.equals(Op.MIN)) {
        	value = Math.min(value, afieldVal);
        }
        else if(op.equals(Op.MAX)) {
        	value = Math.max(value, afieldVal);
        }
        else if(op.equals(Op.COUNT)) {
        	value += 1;
        }
        aggregateMap.put(gbField, value);
        countMap.put(gbField, count);
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new IntegerAggrOpIterator(aggregateMap, countMap, gbfieldType);
    }
    
    class IntegerAggrOpIterator implements OpIterator {
    	
		private ConcurrentHashMap<Field, Integer> aggregateMap;
    	private ConcurrentHashMap<Field, Integer> countMap;
    	private Type gbfieldType;
    	private TupleDesc td;
    	private Iterator<Map.Entry<Field, Integer>> mapIterator;
    	
    	public IntegerAggrOpIterator(ConcurrentHashMap<Field, Integer> aggreMap, 
    			ConcurrentHashMap<Field, Integer> countMap, Type gbfieldType) {
    		this.countMap = countMap;
    		this.aggregateMap = aggreMap;
    		this.gbfieldType = gbfieldType;
    		this.mapIterator = null;
    		Type[] typeAr = this.gbfieldType == null ? new Type[] {Type.INT_TYPE} : new Type[] {this.gbfieldType, Type.INT_TYPE};
    		String[] fieldAr = this.gbfieldType == null ? new String[] {"aggregateVal"} : new String[] {"groupVal", "aggregateVal"};
    		this.td = new TupleDesc(typeAr, fieldAr);
    	}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			mapIterator = this.aggregateMap.entrySet().iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			return mapIterator.hasNext();
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			Map.Entry<Field, Integer> item = mapIterator.next();
			Field gbField = item.getKey();
			int aggrVal = item.getValue();
			int count = this.countMap.get(gbField);
			Tuple tuple = new Tuple(td);
			if (gbField.equals(Aggregator.NO_GROUP_FIELD)) {
				IntField valField = op.equals(Op.AVG) ? new IntField(aggrVal / count) : new IntField(aggrVal);
				tuple.setField(0, valField);
			} else {
				IntField valField = op.equals(Op.AVG) ? new IntField(aggrVal / count) : new IntField(aggrVal);
				tuple.setField(0, gbField);
				tuple.setField(1, valField);
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
