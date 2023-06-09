package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    int tableId;
    TransactionId transId;
    String tableAlias;
    boolean isOpen = false;
    DbFileIterator hfIter;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // TODO:some code goes here
        this.tableAlias = tableAlias;
        this.transId = tid;
        this.tableId = tableid;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // TODO:some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // TODO:some code goes here
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO:some code goes here
        this.isOpen = true;
        this.hfIter = Database.getCatalog().getDatabaseFile(this.tableId).iterator(this.transId);
        this.hfIter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // TODO:some code goes here
        TupleDesc.TDItem[] originalItemList = Database.getCatalog().getTupleDesc(this.tableId).TDItemList;
        TupleDesc.TDItem[] prefixItemList = new TupleDesc.TDItem[originalItemList.length];

        for (int i = 0; i < originalItemList.length; i++) {
            prefixItemList[i] = new TupleDesc.TDItem(originalItemList[i].fieldType,
                    this.tableAlias + "." + originalItemList[i].fieldName);

        }
        return new TupleDesc(prefixItemList);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // TODO:some code goes here
        if (!isOpen) throw new DbException("Seq Scan not open yet");
        return hfIter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO:some code goes here
        if (hasNext()) return hfIter.next();
        else throw new NoSuchElementException();
    }

    public void close() {
        this.isOpen = false;
        this.hfIter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.hfIter.rewind();
    }
}
