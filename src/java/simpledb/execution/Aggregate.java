package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private final OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private final Aggregator aggregator;
    private final Type gfieldType;
    private final TupleDesc td;
    private OpIterator iter;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // TODO: some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.gfieldType = this.gfield == Aggregator.NO_GROUPING ? null : this.child.getTupleDesc().getFieldType(this.gfield);
        if (this.gfieldType == null)
            this.td = new TupleDesc(new Type[] {Type.INT_TYPE});
        else
            this.td = new TupleDesc(new Type[] {this.gfieldType, Type.INT_TYPE});
        if (this.child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE)
            this.aggregator = new IntegerAggregator(gfield, this.gfieldType, afield, aop);
        else
            this.aggregator = new StringAggregator(gfield, this.gfieldType, afield, aop);
        this.iter = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // TODO: some code goes here
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        // TODO: some code goes here
        if (this.gfield != Aggregator.NO_GROUPING)
            return this.child.getTupleDesc().getFieldName(this.gfield);
        return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // TODO: some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        // TODO: some code goes here
        return this.child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // TODO: some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        this.child.open();
        while (this.child.hasNext()) {
            this.aggregator.mergeTupleIntoGroup(child.next());
        }
        this.child.close();
        super.open();
        this.iter = this.aggregator.iterator();
        this.iter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (this.iter.hasNext())
            return iter.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        this.iter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.td;
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        this.iter.close();
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
