package simpledb;

import java.util.*;

import static simpledb.Aggregator.NO_GROUPING;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    Aggregator.Op aggOp;
    private final Aggregator agg;
    private OpIterator aggIter;
    OpIterator child;
    int aggField;
    int gbField;
    boolean isOpen;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // some code goes here
        this.child = child;
        this.aggField = afield;
        this.gbField = gfield;
        this.aggOp = aop;
        this.isOpen = false;

        Type aggFieldType = child.getTupleDesc().getFieldType(afield);
        Type gbFieldType = null;
        if (gbField != NO_GROUPING)
            gbFieldType = child.getTupleDesc().getFieldType(gfield);
        // String/Integer aggOp -> this.agg
        if (aggFieldType == Type.INT_TYPE) {
            this.agg = new IntegerAggregator(gbField, gbFieldType, afield, aop);
        } else {
            this.agg = new StringAggregator(gbField, gbFieldType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return this.gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	    if (this.gbField == NO_GROUPING) return null;
        else return child.getTupleDesc().getFieldName(gbField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return this.aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(aggField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return this.aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    super.open();
        this.child.open();
        this.isOpen = true;
        // Merge tuples into this
        initializeAgg();
        this.aggIter.open();
    }

    /**
     * Initialize Aggregator HashMap. Merging all tuples into aggregator
     * @throws TransactionAbortedException
     * @throws DbException
     */
    private void initializeAgg() throws TransactionAbortedException, DbException {
        while (child.hasNext()) this.agg.mergeTupleIntoGroup(child.next());
        // Initialize iterator here, after merging tuples into aggregator
        this.aggIter = this.agg.iterator();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!isOpen) throw new DbException("Aggregate Operator not open yet");
	    if (!aggIter.hasNext()) return null;
        return aggIter.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    this.child.rewind();
        this.aggIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    return agg.iterator().getTupleDesc();
    }

    public void close() {
        this.isOpen = false;
	    super.close();
        this.child.close();
        this.aggIter.close();
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] childrenIters = new OpIterator[1];
        childrenIters[0] = this.child;
        return childrenIters;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    // some code goes here
        this.child = children[0];
    }
    
}
