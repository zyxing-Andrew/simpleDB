package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbField;
    Type gbFieldType;
    int aggField;
    Op op;
    private HashMap<Field, Integer> countAggMap;
    private int count;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.op = what;

        if (gbfield == NO_GROUPING) this.count = 0;
        else this.countAggMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        if (this.gbField == NO_GROUPING) {
            this.count++;
            return;
        }

        StringField tupAggField = (StringField) tup.getField(this.aggField);
        Field tupGbField = tup.getField(this.gbField);

        int count = countAggMap.getOrDefault(tupGbField, 0);
        countAggMap.put(tupGbField, count + 1);
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
        // some code goes here
        return new OpIterator() {
            boolean isOpen = false;
            Iterator<Field> countMapIter;
            boolean noGroupingHasNext;
            TupleDesc td;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.isOpen = true;
                // System.out.println(countAggMap);
                if (gbField != NO_GROUPING) {
                    this.countMapIter = countAggMap.keySet().iterator();
                    td = new TupleDesc(
                            new Type[] {gbFieldType, Type.INT_TYPE},
                            new String[] {gbFieldType.toString(), Type.INT_TYPE.toString()}
                    );
                } else {
                    noGroupingHasNext = true;
                    td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {Type.INT_TYPE.toString()});
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen) throw new DbException("StringAgg Iterator has not open yet");
                // No grouping only retrieve once
                if (gbField == NO_GROUPING) return noGroupingHasNext;
                return countMapIter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!isOpen) throw new DbException("StringAgg Iterator has not open yet");

                Tuple nextTuple = new Tuple(td);
                if (gbField == NO_GROUPING) {
                    nextTuple.setField(0, new IntField(count));
                    noGroupingHasNext = false;
                } else {
                    Field nextGbField = countMapIter.next();
                    nextTuple.setField(0, nextGbField);
                    nextTuple.setField(1, new IntField(countAggMap.get(nextGbField)));
                }
                // System.out.println(nextTuple);
                return nextTuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!isOpen) throw new DbException("IntegerAgg Iterator has not open yet");

                if (gbField != NO_GROUPING) {
                    this.countMapIter = countAggMap.keySet().iterator();
                } else {
                    noGroupingHasNext = true;
                }
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                isOpen = false;
            }
        };
    }

}
