package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private final int numBuckets;
    private final int min;
    private final int max;
    private final int width;
    private long sum;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // TODO: some code goes here
        this.numBuckets = buckets;
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (max - min) / buckets + 1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // TODO: some code goes here
        this.buckets[(v - this.min) / this.width]++;
        sum += v;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // TODO: some code goes here
        if (v < this.min) {
            switch (op) {
                case EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return 0.0;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                case NOT_EQUALS:
                    return 1.0;
            }
        } else if (v > this.max) {
            switch (op) {
                case NOT_EQUALS:
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return 1.0;
                case EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return 0.0;
            }
        }

        int bucket = (v - this.min) / this.width;
        int estimate = 0;
        switch (op) {
            case EQUALS:
            case LIKE:
                estimate = this.buckets[bucket] / this.width;
                break;
            case LESS_THAN:
                for (int i = 0; i < bucket; i++) {
                    estimate += this.buckets[i];
                }
                estimate += this.buckets[bucket] / this.width * ((v - this.min) % this.width);
                break;
            case LESS_THAN_OR_EQ:
                for (int i = 0; i < bucket; i++) {
                    estimate += this.buckets[i];
                }
                estimate += this.buckets[bucket] / this.width * ((v - this.min) % this.width + 1);
                break;
            case GREATER_THAN:
                for (int i = this.numBuckets - 1; i >= bucket; i--) {
                    estimate += this.buckets[i];
                }
                estimate -= this.buckets[bucket] / this.width * ((v - this.min) % this.width + 1);
                break;
            case GREATER_THAN_OR_EQ:
                for (int i = this.numBuckets - 1; i >= bucket; i--) {
                    estimate += this.buckets[i];
                }
                estimate -= this.buckets[bucket] / this.width * ((v - this.min) % this.width);
                break;
            case NOT_EQUALS:
                estimate = this.getNumTuples() - this.buckets[bucket] / this.width;
                break;
            default:
                break;
        }
        return estimate * 1.0 / this.getNumTuples();
    }

    /**
     * @return the average selectivity of this histogram.
     *         <p>
     *         This is not an indispensable method to implement the basic
     *         join optimization. It may be needed if you want to
     *         implement a more efficient optimization
     */
    public double avgSelectivity(Predicate.Op op) {
        // TODO: some code goes here
        return estimateSelectivity(op, (int) (sum / getNumTuples()));
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return "NumberOfBuckets: " + this.numBuckets +
                ", NumberOfTuples: " + getNumTuples() +
                ", Min: " + this.min +
                ", Max: " + this.max +
                ", Histogram: " + Arrays.toString(this.buckets);
    }

    private int getNumTuples() {
        return Arrays.stream(buckets).sum();
    }
}
