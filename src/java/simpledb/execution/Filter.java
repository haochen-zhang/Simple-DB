package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private final Predicate predicate;
    private OpIterator iterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // TODO: some code goes here
        this.predicate = p;
        this.iterator = child;
    }

    public Predicate getPredicate() {
        // TODO: some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.iterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        this.iterator.open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        this.iterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        this.iterator.rewind();
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
        while (this.iterator.hasNext()) {
            Tuple t = this.iterator.next();
            if (this.predicate.filter(t)) {
                return t;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{this.iterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        if (this.iterator != children[0]) {
            this.iterator = children[0];
        }
    }
}
