package jgea.metrics;

import io.github.ericmedvet.jgea.core.distance.Distance;
import jgea.query.MainQuery;

/**
 * Calculates the similarity between the performance profile of the original query
 * and the query executed on the anonymized stream.
 *
 * The metric is based on the Normalized Euclidean Distance of the relative errors.
 * It is calibrated so that:
 * 1. Identical performance yields a score of 1.0.
 * 2. Tripling the data volume (3x load) is considered the "Worst Case" and yields 0.0.
 * 3. Empty streams represent a "Halfway Error" and yield 0.5.
 */
public class PerformanceSimilarity implements Distance<MainQuery.PerformanceMetrics> {

    // Tripling data means: |Orig - 3*Orig| / Orig = |-2| = Relative Error of 2.0.
    // Assuming this error occurs on all 8 metrics, the Euclidean distance is:
    // sqrt(8 * 2.0^2) = sqrt(8 * 4) = sqrt(32).
    // In case of only filters we have as max distance the empty stream
    //private static final double MAX_TOLERABLE_DISTANCE = Math.sqrt(8);
    private static final double MAX_TOLERABLE_DISTANCE = Math.sqrt(32);

    @Override
    public Double apply(MainQuery.PerformanceMetrics o, MainQuery.PerformanceMetrics m) {

        // Convert records to arrays for easier iteration
        long[] orig = {
                o.afterSource(), o.beforeFilter1(), o.afterFilter1(),
                o.beforeAggregate(), o.afterAggregate(),
                o.beforeFilter2(), o.afterFilter2(), o.beforeSink()
        };

        long[] mod = {
                m.afterSource(), m.beforeFilter1(), m.afterFilter1(),
                m.beforeAggregate(), m.afterAggregate(),
                m.beforeFilter2(), m.afterFilter2(), m.beforeSink()
        };

        double sumSquaredRelativeError = 0;

        for (int i = 0; i < orig.length; i++) {
            double relativeError;

            if (orig[i] == 0) {
                // Edge case: if baseline is 0
                // If modified is also 0, error is 0
                relativeError = (mod[i] == 0) ? 0.0 : 1.0;
            } else {
                // Calculate Relative Error with respect to the Baseline.
                relativeError = Math.abs(orig[i] - mod[i]) / (double) orig[i];
            }

            // Accumulate the square of the error
            sumSquaredRelativeError += relativeError * relativeError;
        }

        // Calculate the total geometric distance
        double distance = Math.sqrt(sumSquaredRelativeError);

        // Cutoff: If the distance exceeds the Triple Data threshold, return 0.0.
        if (distance >= MAX_TOLERABLE_DISTANCE) {
            return 0.0;
        }

        // Linear Normalization
        return 1.0 - (distance / MAX_TOLERABLE_DISTANCE);
    }
}