package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A list of TDItem describe this Tuple
     */
    public TDItem[] TDItemList;

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

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof TDItem)) return false;

            TDItem tdItem = (TDItem) o;

            /** Feedback for Lab1
             *  Fix bugs for td comparing
            if (tdItem.fieldName == null && this.fieldName == null &&
                    tdItem.fieldType == this.fieldType)
                return true;

            if ((this.fieldName == null && tdItem.fieldName != null) ||
                    (this.fieldName != null && tdItem.fieldName == null))
                return false;
             */

            return tdItem.fieldType.equals(this.fieldType);
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // TODO:some code goes here
        final TDItem[] TDItems = this.TDItemList;

        return new Iterator<TDItem>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < TDItems.length;
            }

            @Override
            public TDItem next() {
                if (hasNext()) {
                    TDItem nextTDItem = TDItems[i];
                    i++;
                    return nextTDItem;
                }
                throw new NoSuchElementException("No more index available");
            }
        };
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
        // TODO:some code goes here
        TDItemList = new TDItem[typeAr.length];

        // Initialize Item List
        for (int i = 0; i < typeAr.length; i++) {
            if (fieldAr[i] != null) this.TDItemList[i] = new TDItem(typeAr[i], fieldAr[i]);
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
        // TODO:some code goes here
        TDItemList = new TDItem[typeAr.length];

        for (int i = 0; i < typeAr.length; i++) {
            this.TDItemList[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * Constructor. Create a new tuple dec with given TDItemList
     * @param _TDItemList
     * Specified array to initialize a new tuple desc
     */
    public TupleDesc(TDItem[] _TDItemList) {
        TDItemList = _TDItemList;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // TODO:some code goes here
        return this.TDItemList.length;
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
    public String getFieldName(int i) throws NoSuchElementException {
        // TODO:some code goes here
        if (i < 0 || i >= TDItemList.length)
            throw new NoSuchElementException("i is not a valid field reference");
        return this.TDItemList[i].fieldName;
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
    public Type getFieldType(int i) throws NoSuchElementException {
        // TODO:some code goes here
        if (i < 0 || i >= TDItemList.length)
            throw new NoSuchElementException("i is not a valid field reference");
        return this.TDItemList[i].fieldType;
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
        // TODO:some code goes here
        int index = -1;
        for (int i = 0; i < this.TDItemList.length; i++) {
            if (TDItemList[i].fieldName != null &&
                    TDItemList[i].fieldName.equals(name)) {
                index = i;
                return index;
            }
        }
        throw new NoSuchElementException("no field with a matching name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // TODO:some code goes here
        int size = 0;
        for (TDItem item : this.TDItemList) {
            size += item.fieldType.getLen();
        }
        return size;
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
        // TODO:some code goes here
        TDItem[] mergedTDList = new TDItem[td1.TDItemList.length + td2.TDItemList.length];
        System.arraycopy(td1.TDItemList, 0, mergedTDList, 0, td1.TDItemList.length);
        System.arraycopy(td2.TDItemList, 0, mergedTDList, td1.TDItemList.length, td2.TDItemList.length);
        return new TupleDesc(mergedTDList);
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

    @Override
    public boolean equals(Object o) {
        // TODO:some code goes here
        if (o == this) return true;
        if (!(o instanceof TupleDesc)) return false;

        TupleDesc td = (TupleDesc) o;

        if (td.TDItemList.length != this.TDItemList.length) return false;
        for (int i = 0; i < td.TDItemList.length; i++) {
            if (!this.TDItemList[i].equals(td.TDItemList[i])) return false;
        }

        return true;
    }

    public int hashCode() {
        // TODO:
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
        // TODO:some code goes here
        LinkedList<String> itemList = new LinkedList<>();
        for (TDItem item : this.TDItemList) {
            if (item.fieldName == null)
                itemList.add(item.fieldType + "(" + "unnamed" + ")");
            itemList.add(item.fieldType + "(" + item.fieldName + ")");
        }
        return String.join(", ", itemList);
    }
}
