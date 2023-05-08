package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private static int tableid;
    private String tableAlias;

    private DbFileIterator i = null;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // TODO: some code goes here
        this.transactionId = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        i = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // TODO: some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // TODO: some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        i.open();
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
        // TODO: some code goes here
        TupleDesc originalTd = Database.getCatalog().getTupleDesc(this.tableid);
        Type[] newFieldType = new Type[originalTd.numFields()];
        String[] newFieldName = new String[originalTd.numFields()];
        for (int i = 0; i < originalTd.numFields(); i++) {
            newFieldType[i] = originalTd.getFieldType(i);
            if (originalTd.getFieldName(i) != null) {
                newFieldName[i] = this.tableAlias + "." + originalTd.getFieldName(i);
            }
        }
        return new TupleDesc(newFieldType, newFieldName);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        return i != null && i.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: some code goes here
        return i.next();
    }

    public void close() {
        // TODO: some code goes here
        i.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        i.rewind();
    }
}
