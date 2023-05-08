package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.DbFile;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private final int ioCostPerPage;
    private final DbFile f;
    private final String name;
    private final Map<Integer, Object> hist = new HashMap<>();
    private int numTups;
    private final TupleDesc tupleDesc;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // TODO: some code goes here
        this.name = Database.getCatalog().getTableName(tableid);
        this.f = Database.getCatalog().getDatabaseFile(tableid);
        this.ioCostPerPage = ioCostPerPage;
        this.tupleDesc = f.getTupleDesc();

//        SeqScan scan = new SeqScan(new TransactionId(), tableid);
        DbFileIterator scan = f.iterator(new TransactionId());
        try {
            int[] minVals = new int[tupleDesc.numFields()];
            int[] maxVals = new int[tupleDesc.numFields()];
            Arrays.fill(minVals, Integer.MAX_VALUE);
            Arrays.fill(maxVals, Integer.MIN_VALUE);

            scan.open();
            while(scan.hasNext()) {
                Tuple tup = scan.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tup.getField(i).getType() == Type.INT_TYPE) {
                        int val = ((IntField) tup.getField(i)).getValue();
                        minVals[i] = Math.min(minVals[i], val);
                        maxVals[i] = Math.max(maxVals[i], val);
                    }
                }
            }
            for(int i = 0; i < tupleDesc.numFields(); i++) {
                if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                    hist.put(i, new IntHistogram(NUM_HIST_BINS, minVals[i], maxVals[i]));
                } else {
                    hist.put(i, new StringHistogram(NUM_HIST_BINS));
                }
            }
            scan.rewind();
            while(scan.hasNext()) {
                Tuple tup = scan.next();
                this.numTups++;
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tup.getField(i).getType() == Type.INT_TYPE) {
                        ((IntHistogram) hist.get(i)).addValue(((IntField) tup.getField(i)).getValue());
                    } else {
                        ((StringHistogram) hist.get(i)).addValue(((StringField) tup.getField(i)).getValue());
                    }
                }
            }
            scan.close();
        } catch (DbException | TransactionAbortedException e) {
            throw new RuntimeException(e.getMessage());
        }

        setTableStats(name, this);
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // TODO: some code goes here
        return this.f.numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // TODO: some code goes here
        return (int) Math.ceil(this.numTups * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        if (this.tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            return ((IntHistogram) hist.get(field)).avgSelectivity(op);
        } else {
            return ((StringHistogram) hist.get(field)).avgSelectivity(op);
        }
    }

    public double avgSelectivity(String fieldName, Predicate.Op op) {
        int field = -1;
        for(int i = 0; i < this.tupleDesc.numFields(); i++) {
            if (this.tupleDesc.getFieldName(i).equals(fieldName)) {
                field = i;
            }
        }
        return avgSelectivity(field, op);
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // TODO: some code goes here
        if (constant.getType() == Type.INT_TYPE) {
            return ((IntHistogram) hist.get(field)).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            return ((StringHistogram) hist.get(field)).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // TODO: some code goes here
        return this.numTups;
    }

}
