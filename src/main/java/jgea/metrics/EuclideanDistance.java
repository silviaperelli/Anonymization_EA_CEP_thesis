package jgea.metrics;

import io.github.ericmedvet.jgea.core.distance.Distance;
import jgea.query.MainQuery;

// Calculate Euclidean distance used as fitness metric to quantify the difference between the performance profile
// of the main query running on the original stream and on a modified stream
public class EuclideanDistance implements Distance<MainQuery.PerformanceMetrics> {

    @Override
    public Double apply(MainQuery.PerformanceMetrics originalMetrics, MainQuery.PerformanceMetrics newMetrics) {

        double sumOfSquares = 0;

        // For each performance counter, calculate the squared difference and add it to the total sum
        sumOfSquares += Math.pow((double) originalMetrics.afterFilter1() - newMetrics.afterFilter1(), 2);
        sumOfSquares += Math.pow((double) originalMetrics.beforeAggregate() - newMetrics.beforeAggregate(), 2);
        sumOfSquares += Math.pow((double) originalMetrics.afterAggregate() - newMetrics.afterAggregate(), 2);
        sumOfSquares += Math.pow((double) originalMetrics.beforeFilter2() - newMetrics.beforeFilter2(), 2);
        sumOfSquares += Math.pow((double) originalMetrics.afterFilter2() - newMetrics.afterFilter2(), 2);
        sumOfSquares += Math.pow((double) originalMetrics.beforeSink() - newMetrics.beforeSink(), 2);
        sumOfSquares += Math.pow((double) originalMetrics.afterSource() - newMetrics.afterSource(), 2);
        sumOfSquares += Math.pow((double) originalMetrics.beforeFilter1() - newMetrics.beforeFilter1(), 2);
        // Square root of the sum of squared differences
        return Math.sqrt(sumOfSquares);
    }
}
