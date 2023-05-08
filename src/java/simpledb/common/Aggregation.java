package simpledb.common;

import simpledb.execution.Aggregator;

/**
 * Helper class to store the aggregation result
 */
public class Aggregation {
    private final Aggregator.Op what;
    private int count;
    private int aggregatedResult;
    private int sum;

    public Aggregation(Aggregator.Op what) {
        this.what = what;
        this.count = 0;
    }

    public int getAggregatedResult() {
        return this.aggregatedResult;
    }

    public void updateResult(int aVal) {
        this.count++;
        switch (this.what) {
            case COUNT:
                this.aggregatedResult = this.count;
                break;
            case SUM:
                this.sum += aVal;
                this.aggregatedResult = this.sum;
                break;
            case AVG:
                this.sum += aVal;
                this.aggregatedResult = this.sum / this.count;
                break;
            case MIN:
                if (this.count == 1)
                    this.aggregatedResult = aVal;
                else
                    this.aggregatedResult = Math.min(aVal, this.aggregatedResult);
                break;
            case MAX:
                if (this.count == 1)
                    this.aggregatedResult = aVal;
                else
                    this.aggregatedResult = Math.max(aVal, this.aggregatedResult);
                break;
        }
    }
}
