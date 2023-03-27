package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    public RecordId recordId;
    public Field[] fields;
    public TupleDesc td;
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param _td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc _td) {
        // TODO:some code goes here
        this.td = _td;
        this.fields = new Field[_td.TDItemList.length];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // TODO:some code goes here
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // TODO:some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // TODO:some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // TODO:some code goes here
        this.fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // TODO:some code goes here
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // TODO:some code goes here
        LinkedList<String> fieldList = new LinkedList<>();
        for (Field f : fields) {
            fieldList.add(f.toString());
        }
        return String.join(" ", fieldList);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof Tuple)) return false;

        Tuple other = (Tuple) o;
        if (!other.td.equals(this.td) || !other.recordId.equals(this.recordId)) return false;
        if (other.fields.length != this.fields.length) return false;
        for (int i = 0; i < fields.length; i++) {
            if (!other.fields[i].equals(fields[i])) return false;
        }
        return true;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // TODO:some code goes here
        final Field[] _fields = this.fields;
        return new Iterator<Field>() {
            int i = 0;
            @Override
            public boolean hasNext() {
                return i < _fields.length;
            }

            @Override
            public Field next() {
                if (hasNext()) {
                    Field nextField = _fields[i];
                    i++;
                    return nextField;
                }
                throw new NoSuchElementException("No more index available");
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc _td)
    {
        // TODO:some code goes here
        this.td = _td;
    }
}
