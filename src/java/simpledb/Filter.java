package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
    Predicate pred;
    OpIterator child;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */

    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.pred = p;
        this.child = child;

    }

    public Predicate getPredicate() {
        // some code goes here
        return this.pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (!child.hasNext()) return null;

        Tuple filterTuple = child.next();
        // Jump over un-satisfied tuple
        while (!pred.filter(filterTuple)) {
            if (child.hasNext()) filterTuple = child.next();
            else return null;
        }
        return filterTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
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
