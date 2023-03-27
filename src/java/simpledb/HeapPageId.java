package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    int tableId;
    int pageNo;
    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // TODO:some code goes here
        this.tableId = tableId;
        this.pageNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // TODO:some code goes here
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // TODO:some code goes here
        return this.pageNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // TODO:some code goes here
        int sign = 1;
        if (this.tableId < 0) sign = -sign;
        if (this.pageNo < 0) sign = -sign;

        String tableIdString = Integer.toString(Math.abs(tableId));
        String pageNoString = Integer.toString(Math.abs(pageNo));
        // Prevent int overflow
        if (tableIdString.length() >= 8) {
            // tableId + pageNo
            //System.out.println(sign * (Math.abs(tableId) + Math.abs(pageNo)));
            return sign * (Math.abs(tableId) + Math.abs(pageNo));
        } else {
            // "tableId" + "pageNo"
            //System.out.println(tableIdString + ", " + pageNoString);
            String concat = tableIdString + pageNoString;
            return sign * Integer.parseInt(concat);
        }
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // TODO:some code goes here
        if (o == this) return true;
        if (!(o instanceof HeapPageId)) return false;

        HeapPageId hpId = (HeapPageId) o;

        return this.pageNo == hpId.pageNo && this.tableId == hpId.tableId;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
