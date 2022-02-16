package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.rowset.JoinRowSet;

import org.junit.experimental.max.MaxCore;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 10;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    /**
	 *   For this function, you'll have to get the
	     DbFile for the table in question,
	     then scan through its tuples and calculate
	     the values that you need.
	     You should try to do this reasonably efficiently, but you don't
	     necessarily have to (for example) do everything
	     in a single scan of the table.
     */
    
    private int tableid;
    private int ioCostPerPage;
    private int ntups; 
    private int numPages;
    private TupleDesc td;
    private int[] fieldMaxVal;
    private int[] fieldMinVal;
    private HashMap<Integer, IntHistogram> intFieldHistogramMap;
    private HashMap<Integer, StringHistogram> strFieldHistogramMap;
    
    public TableStats(int tableid, int ioCostPerPage) {
    	this.tableid = tableid;
    	this.ioCostPerPage = ioCostPerPage;
    	TransactionId tid = new TransactionId();
    	SeqScan scan = new SeqScan(tid, tableid);
    	this.numPages = ((HeapFile) Database.getCatalog().getDatabaseFile(tableid)).numPages();
    	this.td = scan.getTupleDesc();
    	this.fieldMaxVal = new int[scan.getTupleDesc().numFields()];
    	this.fieldMinVal = new int[scan.getTupleDesc().numFields()];
    	this.intFieldHistogramMap = new HashMap<Integer, IntHistogram>(scan.getTupleDesc().numFields());
    	this.strFieldHistogramMap = new HashMap<Integer, StringHistogram>(scan.getTupleDesc().numFields());
    	try {
    		// 获取table中tuple的数量和每个字段的最大最小值(忽略string字段)
			scan.open();
			while (scan.hasNext()) {
	    		Tuple tuple = scan.next();
	    		ntups += 1;
	    		for (int fieldIndex = 0; fieldIndex < tuple.getTupleDesc().numFields(); fieldIndex += 1) {
	    			Field field = tuple.getField(fieldIndex);
	    			if (field.getType().equals(Type.STRING_TYPE)) {
	    				continue;
	    			}
	    			if (ntups == 1) {
	    				fieldMaxVal[fieldIndex] = ((IntField) field).getValue();
	    				fieldMinVal[fieldIndex] = ((IntField) field).getValue();
	    			} else {
	    				fieldMaxVal[fieldIndex] = Math.max(fieldMaxVal[fieldIndex], ((IntField) field).getValue());
	    				fieldMinVal[fieldIndex] = Math.min(fieldMinVal[fieldIndex], ((IntField) field).getValue());
	    			}
	    		}
	    	}
			// 初始化intFieldHistogramMap和strFieldHistogramMap
			for (int fieldIndex = 0; fieldIndex < scan.getTupleDesc().numFields(); fieldIndex += 1) {
				if (scan.getTupleDesc().getFieldType(fieldIndex).equals(Type.INT_TYPE)) {
					IntHistogram histogram = new IntHistogram(NUM_HIST_BINS, fieldMinVal[fieldIndex], fieldMaxVal[fieldIndex]);
					intFieldHistogramMap.put(fieldIndex, histogram);
				} else {
					StringHistogram histogram = new StringHistogram(NUM_HIST_BINS);
					strFieldHistogramMap.put(null, histogram);
				}
			}
			scan.rewind();
			while (scan.hasNext()) {
				Tuple tuple = scan.next();
				for (int fieldIndex = 0; fieldIndex < tuple.getTupleDesc().numFields(); fieldIndex += 1) {
					Field field = tuple.getField(fieldIndex);
					if (field.getType().equals(Type.INT_TYPE)) {
						intFieldHistogramMap.get(fieldIndex).addValue(((IntField) field).getValue());
					} else {
						strFieldHistogramMap.get(fieldIndex).addValue(((StringField) field).getValue());
					}
				}
			}
			
		} catch (DbException | TransactionAbortedException e) {
			e.printStackTrace();
		}
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return numPages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (ntups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        th e index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        if (td.getFieldType(field).equals(Type.INT_TYPE)) {
        	return intFieldHistogramMap.get(field).avgSelectivity();
        } else {
        	return strFieldHistogramMap.get(field).avgSelectivity();
        }
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (constant.getType().equals(Type.INT_TYPE)) {
        	return intFieldHistogramMap.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
        	return strFieldHistogramMap.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return ntups;
    }

}
