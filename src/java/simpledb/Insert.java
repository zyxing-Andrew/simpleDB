package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId tid;
    OpIterator child;
    int tableId;
    private boolean hasReturned;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        // TupleDesc of child differs from table into which we are to insert
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("Wrong insert tuple format");

        this.tid = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.hasReturned = false;
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
        this.hasReturned = false;
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        TupleDesc singleIntDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        Tuple resultTup = new Tuple(singleIntDesc);

        // Insert operator only return once(in one lifecycle)
        // No matter the child has next or not(return {0})
        if (hasReturned) return null;

        int count = 0;
        while (child.hasNext()) {
            Tuple insertTup = child.next();
            try {
                Database.getBufferPool().insertTuple(this.tid, tableId, insertTup);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }
        // System.out.println(count);
        resultTup.setField(0, new IntField(count));
        hasReturned = true;
        return resultTup;
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
