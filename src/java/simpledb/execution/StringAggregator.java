package simpledb.execution;

import simpledb.common.Aggregation;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final int afield;
    private final Type gbfieldtype;
    private final Op what;
    private final HashMap<Field, Aggregation> aggrMap = new HashMap<>();
    private final TupleDesc td;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (this.gbfield == Aggregator.NO_GROUPING)
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        else
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        Field gbfieldKey;
        if (this.gbfield == Aggregator.NO_GROUPING)
            gbfieldKey = new IntField(0);
        else
            gbfieldKey = tup.getField(this.gbfield);
        aggrMap.computeIfAbsent(gbfieldKey, k -> new Aggregation(what)).updateResult(0);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        List<Tuple> tups = new ArrayList<>();
        aggrMap.forEach((k, v) -> {
            Tuple tup = new Tuple(this.td);
            if (this.gbfield == Aggregator.NO_GROUPING)
                tup.setField(0, new IntField(v.getAggregatedResult()));
            else
                tup.setField(0, k);
            tup.setField(1, new IntField(v.getAggregatedResult()));
            tups.add(tup);
        } );
        return new TupleIterator(this.td, tups);
    }
}
