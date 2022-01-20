package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    /**
     * 字段类型对象: (type, name)
     * 定义字段类型对象数组((type1, name1), (type2, name2), ....)
     */
    private TDItem[] tdItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
	public Iterator<TDItem> iterator() {
        // 返回tdItems数组的迭代器
        return (Iterator<TDItem>)List.of(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // 创建字段类型对象数组
    	tdItems = new TDItem[typeAr.length];
    	for (int tdIndex = 0; tdIndex < typeAr.length; tdIndex ++) {
    		tdItems[tdIndex] = new TDItem(typeAr[tdIndex], fieldAr[tdIndex]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	// 创建字段类型对象数组（匿名字段，名字用""代替）
    	tdItems = new TDItem[typeAr.length];
    	for (int tdIndex = 0; tdIndex < typeAr.length; tdIndex ++) {
    		tdItems[tdIndex] = new TDItem(typeAr[tdIndex], "");
    	}
    }	
    
    public TupleDesc(TDItem[] tdItems) {
		this.tdItems = tdItems;
	}

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int tdIndex) throws NoSuchElementException {
        try {
        	return tdItems[tdIndex].fieldName;
        } catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException(e);
		}
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int tdIndex) throws NoSuchElementException {
    	try {
        	return tdItems[tdIndex].fieldType;
        } catch (ArrayIndexOutOfBoundsException e) {
			throw new NoSuchElementException(e);
		}
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int tdIndex = 0; tdIndex < tdItems.length; tdIndex ++) {
        	if (tdItems[tdIndex].fieldName.equals(name)) {
        		return tdIndex;
        	}
        }
        throw new NoSuchElementException("The index corresponding to the field name: " + name + "could not be found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // 返回字段类型对象数组的总大小
        int tdItemsSize = 0;
        for (TDItem tdItem: tdItems) {
        	tdItemsSize += tdItem.fieldType.getLen();
        }
        return tdItemsSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // 合并两个数组对象组为一个字段类型对象数组
    	int tdLength = td1.numFields() + td2.numFields();
    	int td1Length = td1.numFields();
    	TDItem[] newTDItems = new TDItem[tdLength];
    	for (int tdIndex = 0; tdIndex < tdLength; tdIndex ++) {
    		if (tdIndex < td1.numFields()) {
    			newTDItems[tdIndex] = new TDItem(td1.getFieldType(tdIndex), td1.getFieldName(tdIndex));
    		} else {
    			newTDItems[tdIndex] = new TDItem(td2.getFieldType(tdIndex - td1Length), td2.getFieldName(tdIndex - td1Length));
    		}
    	}
    	return new TupleDesc(newTDItems);
    }

	/**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object obj) {
        if (!(obj instanceof TupleDesc)) {
        	return false;
        }
        TupleDesc tupleDescIns = (TupleDesc) obj;
        if (tupleDescIns.numFields() != this.numFields()) {
        	return false;
        }
        for (int tdIndex = 0; tdIndex < tupleDescIns.numFields(); tdIndex ++) {
        	if (!(tupleDescIns.getFieldName(tdIndex).equals(this.getFieldName(tdIndex)))) {
        		return false;
        	}
        	if (tupleDescIns.getFieldType(tdIndex) != this.getFieldType(tdIndex)) {
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String tupleDescString = "";
        for (int tdIndex = 0; tdIndex < this.numFields(); tdIndex ++) {
        	tupleDescString += this.getFieldType(tdIndex) + "(" + this.getFieldName(tdIndex) + ")";
        	if (tdIndex != this.numFields() - 1) {
        		tupleDescString += ", ";
        	}
        }
        return tupleDescString;
    }
}
