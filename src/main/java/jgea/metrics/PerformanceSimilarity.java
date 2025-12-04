package jgea.metrics;

import io.github.ericmedvet.jgea.core.distance.Distance;
import jgea.query.MainQuery;
import jgea.query.MainQueryKeys;

/**
 * Calculates the similarity between the performance profile of the original query
 * and the query executed on the anonymized stream.
 *
 * The profile is represented as a 12-dimensional vector, comprising 8 metrics for
 * tuple counts and 4 metrics for unique key (sensor) counts
 *
 * The metric is based on the Normalized Euclidean Distance of the relative errors.
 * It is calibrated so that:
 * 1. A score of 1.0 indicates identical performance profiles (zero distance).
 * 2. A score of 0.0 is assigned if the distance reaches or exceeds a calibrated 'worst-case' threshold.
 */
public class PerformanceSimilarity implements Distance<MainQueryKeys.PerformanceMetrics> {

    /**
     * The maximum tolerable Euclidean distance, used to normalize the similarity score.
     * This value is calibrated on a hybrid and realistic 'worst-case' scenario:
     *      For the 8 tuple metrics: the worst case is a tripling of the data volume.
     *      |Orig - 3*Orig| / Orig = |-2| = Relative Error of 2.0.
     *      For the 4 key metrics: the worst case is the suppression of all keys (value goes to 0),
     *      |Orig - 0*Orig| / Orig = |1| = Relative Error of 1.0.
     * The resulting maximum distance is sqrt(36) = 6.0}
     */
    private static final double MAX_TOLERABLE_DISTANCE = 6.0;
    // In case of only filters we have as max distance the empty stream
    //private static final double MAX_TOLERABLE_DISTANCE = Math.sqrt(12);

    @Override
    public Double apply(MainQueryKeys.PerformanceMetrics o, MainQueryKeys.PerformanceMetrics m) {

        // Convert records to arrays for easier iteration
        long[] originalTuples = {
                o.afterSource(), o.beforeFilter1(), o.afterFilter1(),
                o.beforeAggregate(), o.afterAggregate(),
                o.beforeFilter2(), o.afterFilter2(), o.beforeSink()
        };

        long[] modifiedTuples = {
                m.afterSource(), m.beforeFilter1(), m.afterFilter1(),
                m.beforeAggregate(), m.afterAggregate(),
                m.beforeFilter2(), m.afterFilter2(), m.beforeSink()
        };

        long[] originalKeys = {
                o.keysAfterSource(), o.keysAfterFilter1(), o.keysAfterAggregate(), o.keysOutput()
        };
        long[] modifiedKeys = {
                m.keysAfterSource(), m.keysAfterFilter1(), m.keysAfterAggregate(), m.keysOutput()
        };

        double sumSquaredRelativeError = 0;

        // Calculate the sum of squared errors across all 12 dimensions
        for (int i = 0; i < originalTuples.length; i++) {
            sumSquaredRelativeError += calculateSquaredRelativeError(originalTuples[i], modifiedTuples[i]);
        }

        for (int i = 0; i < originalKeys.length; i++) {
            sumSquaredRelativeError += calculateSquaredRelativeError(originalKeys[i], modifiedKeys[i]);
        }

        // Calculate the final Euclidean distance
        double distance = Math.sqrt(sumSquaredRelativeError);

        // Cutoff: If the distance exceeds the maximum threshold, return 0.0.
        if (distance >= MAX_TOLERABLE_DISTANCE) {
            return 0.0;
        }

        // Normalize the distance
        return 1.0 - (distance / MAX_TOLERABLE_DISTANCE);

    }


    // Helper method to calculate the squared relative error between two values
    private double calculateSquaredRelativeError(long original, long modified) {
        double relativeError;
        if (original == 0) {
            relativeError = (modified == 0) ? 0.0 : 1.0;
        } else {
            relativeError = Math.abs(original - modified) / (double) original;
        }
        return relativeError * relativeError;
    }

}