package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbField;
    Type gbFieldType;
    int aggField;
    Op op;

    /**
     * <Group-By Field - Aggregate Result>
     * Recording the result of aggregation, using double
     * as value, since we'll need to calculate average result
     */
    private HashMap<Field, Double> resultAggMap;
    // <Group-By Field - Aggregate Count>
    // Specific to AVG calculation
    private HashMap<Field, Integer> countAggMap;

    // No-grouping result, count
    private double result;
    private int count;

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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggField = afield;
        this.op = what;

        if (gbfield == NO_GROUPING) {
            this.count = 0;
            if (what == Op.MIN) result = Integer.MAX_VALUE;
            else if (what == Op.MAX) result = Integer.MIN_VALUE;
            else result = 0;
        } else {
            resultAggMap = new HashMap<>();
            countAggMap = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        // No-grouping merge
        if (this.gbField == NO_GROUPING) {
            mergeTupleNoGrouping(tup);
            return;
        }

        // Grouping merge
        IntField tupAggField = (IntField) tup.getField(this.aggField);
        Field tupGbField = tup.getField(this.gbField);

        // Aggregate enums:
        if (this.op == Op.MIN) {
            double min = resultAggMap.getOrDefault(tupGbField, Double.MAX_VALUE);
            min = Math.min(tupAggField.getValue(), min);
            resultAggMap.put(tupGbField, min);
        } else if (this.op == Op.MAX) {
            double max = resultAggMap.getOrDefault(tupGbField, Double.MIN_VALUE);
            max = Math.max(tupAggField.getValue(), max);
            resultAggMap.put(tupGbField, max);
        } else if (this.op == Op.AVG) {
            double avg = resultAggMap.getOrDefault(tupGbField, 0.0);
            int count = countAggMap.getOrDefault(tupGbField, 0);
            double sum = (avg * count) + tupAggField.getValue();
            count += 1;
            avg = sum / count;
            this.resultAggMap.put(tupGbField, avg);
            this.countAggMap.put(tupGbField, count);
        } else if (this.op == Op.COUNT) {
            int count = countAggMap.getOrDefault(tupGbField, 0);
            countAggMap.put(tupGbField, count + 1);
            resultAggMap.put(tupGbField, (double) count + 1);
        } else if (this.op == Op.SUM) {
            double sum = resultAggMap.getOrDefault(tupGbField, 0.0);
            resultAggMap.put(tupGbField, (double) sum + tupAggField.getValue());
        }
    }

    /**
     * Function for no-grouping merge
     * Updating single value fields
     * @param tup
     *              tuple needs to merge
     */
    private void mergeTupleNoGrouping(Tuple tup) {
        IntField tupAggField = (IntField) tup.getField(this.aggField);

        if (this.op == Op.MIN) {
            this.result = Math.min(tupAggField.getValue(), result);
        } else if (this.op == Op.MAX) {
            this.result = Math.max(tupAggField.getValue(), result);
        } else if (this.op == Op.SUM) {
            this.result += tupAggField.getValue();
        } else if (this.op == Op.COUNT) {
            this.count++;
            // Keep all result in one field
            this.result = count;
        } else if (this.op == Op.AVG) {
            double sum = result * count + tupAggField.getValue();
            this.count++;
            this.result = sum / count;
        }
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
        // some code goes here
        return new OpIterator() {
            boolean isOpen = false;
            Iterator<Field> resMapIter;
            // Iterator<Field> countMapIter;
            boolean noGroupingHasNext;
            TupleDesc td;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.isOpen = true;
                // System.out.println("result map : " + resultAggMap);
                // System.out.println("result : " + result);
                if (gbField != NO_GROUPING) {
                    this.resMapIter = resultAggMap.keySet().iterator();
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
                if (!isOpen) throw new DbException("IntegerAgg Iterator has not open yet");
                // No grouping only retrieve once
                if (gbField == NO_GROUPING) return noGroupingHasNext;

                return resMapIter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!isOpen) throw new DbException("IntegerAgg Iterator has not open yet");

                if (!this.hasNext()) throw new NoSuchElementException("No more hasNext()");

                Tuple nextTuple = new Tuple(td);
                // No grouping only return single value
                if (gbField == NO_GROUPING) {
                    nextTuple.setField(0, new IntField((int) result));
                    noGroupingHasNext = false;
                }
                else {
                    Field nextGbField = resMapIter.next();
                    nextTuple.setField(0, nextGbField);
                    // cast double value to int
                    nextTuple.setField(1, new IntField((int) Math.floor(resultAggMap.get(nextGbField))));
                }
                return nextTuple;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (!isOpen) throw new DbException("IntegerAgg Iterator has not open yet");

                if (gbField != NO_GROUPING) {
                    this.resMapIter = resultAggMap.keySet().iterator();
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
