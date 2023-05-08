package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private final OpIterator child;
    private int tableId;
    private final TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
    private boolean fetched;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // TODO: some code goes here
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("TupleDesc is mismatch");
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.fetched = false;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        this.child.open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        close();
        open();
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
        // TODO: some code goes here
        if (fetched)
            return null;
        Tuple tup = new Tuple(td);
        int count = 0;
        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, child.next());
                count++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tup.setField(0, new IntField(count));
        fetched = true;
        return tup;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
    }
}
