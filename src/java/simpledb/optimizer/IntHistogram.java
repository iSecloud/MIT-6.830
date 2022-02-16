package simpledb.optimizer;

import javax.management.loading.PrivateClassLoader;

import simpledb.execution.Predicate;
import simpledb.execution.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	
	private int[] buckets;
	private int min;
	private int max;
	private int ntups;
	private double width;
	
    public IntHistogram(int buckets, int min, int max) {
    	this.buckets = new int[buckets];
    	this.min = min;
    	this.max = max;
    	this.ntups = 0;
    	this.width = 1.0 * (max - min + 1) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	if (v < min || v > max) {
    		return;
    	}
    	ntups += 1;
    	buckets[(int) ((v - min) / width)] += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "const" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param const Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int val) {
    	if (op.equals(Predicate.Op.EQUALS)) {
    		// if (val > max || val < min) return 0.0;
    		// return 1.0 * buckets[(int) ((val - min) / width)] / (width * ntups);
    		return estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, val) - estimateSelectivity(Predicate.Op.GREATER_THAN, val);
    	}
    	else if (op.equals(Predicate.Op.GREATER_THAN)) {
    		if (val < min) return 1.0;
    		if (val >= max) return 0.0;
    		int index = (int) ((val - min) / width);
    		// double rate = ((index + 1) * width - val) * estimateSelectivity(op.EQUALS, val);
    		double rate = ((index + 1) * width - val) * 1.0 * buckets[(int) ((val - min) / width)] / (width * ntups);
    		int sum = 0;
    		for (int i = index + 1; i < buckets.length; i += 1) {
    			sum += buckets[i];
    		}
    		rate += (1.0 * sum / ntups);
    		return rate;
    	}
    	else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
    		// return estimateSelectivity(Predicate.Op.GREATER_THAN, val) + estimateSelectivity(Predicate.Op.EQUALS, val);
    		return estimateSelectivity(Predicate.Op.GREATER_THAN, val - 1);
    	}
    	else if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
    		return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN, val);
    	}
    	else if (op.equals(Predicate.Op.LESS_THAN)) {
    		return 1.0 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ, val);
    	}
    	else if (op.equals(Predicate.Op.NOT_EQUALS)) {
    		return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, val);
    	}
    	return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return 1.0 / (max - min + 1);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return "这个直方图有: " + ntups + "个元素，最大值为: " + max + "，最小值为: " + min + "，被分割为了" + buckets.length + "份，宽度为" + width;
    }
}
