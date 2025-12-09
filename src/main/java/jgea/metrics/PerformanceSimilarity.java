package jgea.metrics;

import io.github.ericmedvet.jgea.core.distance.Distance;
import utils.StreamStatsWindow;

/**
 * Calculates the similarity between two performance profiles represented by StreamStatsWindow objects.
 * The similarity is based on the normalized Euclidean Distance of the relative errors,
 * measured at each time bucket for both tuple and key counts.
 *
 * The score is maximized, where:
 * - 1.0 indicates identical performance profiles.
 * - 0.0 indicates a distance equal to or greater than the maximum theoretical distance (D_max).
 */
public class PerformanceSimilarity implements Distance<StreamStatsWindow> {

    private final double dMax;

    /**
     * Il costruttore calcola D_max una sola volta basandosi sul profilo originale.
     */
    // Pre-calculates D_max, so it is computed once based on the timestamp represented on the original performance profile
    public PerformanceSimilarity(StreamStatsWindow originalProfile) {
        if (originalProfile == null) {
            throw new IllegalArgumentException("Original profile cannot be null.");
        }
        this.dMax = calculateDMax(originalProfile);
    }

    @Override
    public Double apply(StreamStatsWindow originalProfile, StreamStatsWindow modifiedProfile) {
        if (modifiedProfile == null) {
            return 0.0;
        }

        // Calculate the sum of squared relative errors by calling the helper method in StreamStatsWindow
        double sumOfSquaredErrors = originalProfile.calculateSumOfSquaredRelativeErrors(modifiedProfile);

        // Compute the final Euclidean distance
        double distance = Math.sqrt(sumOfSquaredErrors);

        // Normalize the result to a [0,1] similarity score
        if (this.dMax == 0) {
            // Avoid division by zero if the original profile was completely empty
            return (distance == 0) ? 1.0 : 0.0;
        }

        double similarity = 1.0 - (distance / this.dMax);
        // Ensure the score is not negative if the distance exceeds D_max
        return Math.max(0.0, similarity);
    }

    /**
     * Helper method to calculate D_max, the distance for the "worst-case scenario".
     * The worst-case is defined as:
     * - For TUPLES: Triplication of data (relative error = 2.0).
     * - For KEYS: Total suppression (relative error = 1.0).
     *
     *  For tuples if original > 0, the worst case is generating data from nothing (error = 1.0)
     * */
    private double calculateDMax(StreamStatsWindow originalProfile) {
        double sumOfWorstCaseSquaredErrors = 0.0;
        final double TUPLE_DISTORTION_ERROR = 2.0;
        final double KEY_WORST_CASE_ERROR = 1.0;
        final double GENERATION_FROM_ZERO_ERROR = 1.0;

        for (String streamName : originalProfile.streamNames()) {
            int[] tupleCounts = originalProfile.getTupleArray(streamName);
            int[] keyCounts = originalProfile.getKeyArray(streamName);

            // Iterate through all time buckets for tuples
            for (int originalCount : tupleCounts) {
                if (originalCount > 0) {
                    // If data existed, the worst case is triplication
                    sumOfWorstCaseSquaredErrors += Math.pow(TUPLE_DISTORTION_ERROR, 2);
                } else {
                    // If no data existed, the worst case is creating data from nothing
                    sumOfWorstCaseSquaredErrors += Math.pow(GENERATION_FROM_ZERO_ERROR, 2);
                }
            }

            // Iterate through all time buckets for keys
            for (int originalCount : keyCounts) {
                // For keys the worst case is suppression because the operators cannot create new keys
                sumOfWorstCaseSquaredErrors += Math.pow(KEY_WORST_CASE_ERROR, 2);
            }
        }
        return Math.sqrt(sumOfWorstCaseSquaredErrors);
    }
}