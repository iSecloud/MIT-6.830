package simpledb.storage;

import java.io.Serializable;
import java.util.Objects;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    private PageId pageId;
    private int tupleNumber;
    
    public RecordId(PageId pageId, int tupleNumber) {
        this.pageId = pageId;
        this.tupleNumber = tupleNumber;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return tupleNumber;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object obj) {
    	// TODO: 是否可以用hashCode来进行判断
        if (!(obj instanceof RecordId)) {
        	return false;
        }
        RecordId recordId = (RecordId)obj;
        if (!recordId.pageId.equals(pageId) || recordId.tupleNumber != tupleNumber) {
        	return false;
        }
        return true;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return Objects.hash(pageId.hashCode(), tupleNumber);
    }

}
