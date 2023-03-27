package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    public PageId pid;
    public int tupNo;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // TODO:some code goes here
        this.pid = pid;
        this.tupNo = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // TODO:some code goes here
        return this.tupNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // TODO:some code goes here
        return this.pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // TODO:some code goes here
        if (o == this) return true;
        if (!(o instanceof RecordId)) return false;

        RecordId recordId = (RecordId) o;

        return recordId.tupNo == this.tupNo && recordId.pid.equals(this.pid);
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // TODO:some code goes here
        int sign = 1;
        if (this.tupNo < 0) sign = -sign;
        if (this.pid.hashCode() < 0) sign = -sign;

        String tidString = Integer.toString(Math.abs(this.tupNo));
        String pidString = Integer.toString(Math.abs(this.pid.hashCode()));
        String concat = tidString + pidString;
        System.out.println(tidString + "," + pidString);
        System.out.println(concat);
        return sign * Integer.parseInt(concat);
    }

}
