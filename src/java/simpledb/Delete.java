package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    TransactionId tid;
    OpIterator child;
    // Indicate this delete operator in this lifecycle has return result or not
    private boolean hasReturned;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        TupleDesc singleIntDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
        Tuple resultTup = new Tuple(singleIntDesc);

        // Delete operator only return once(in one lifecycle)
        // No matter the child has next or not(return {0})
        if (hasReturned) return null;

        int count = 0;
        while (child.hasNext()) {
            Tuple deleteTup = child.next();
            try {
                Database.getBufferPool().deleteTuple(this.tid, deleteTup);
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
