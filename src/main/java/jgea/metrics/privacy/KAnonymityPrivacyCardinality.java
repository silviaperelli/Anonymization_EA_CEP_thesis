package jgea.metrics.privacy;

import event.AirQualityEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;
import jgea.metrics.privacy.utils.KDTree;
import jgea.metrics.privacy.utils.MetricUtils;
import jgea.query.utils.OperatorUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * A privacy metric that evaluates a modified stream based on two criteria:
 * 1.  Anonymity Quality: For each tuple, it measures the "flatness" of the distance distribution
 *     to its k-nearest neighbors in the original stream. A low standard deviation (flat distribution)
 *     implies higher privacy as it confuses an attacker.
 * 2.  Cardinality Fidelity: It penalizes solutions that significantly alter the size (cardinality)
 *     of the dataset compared to the original.
 *
 * The final score is the product of the average anonymity quality and a size similarity factor
 */
public class KAnonymityPrivacyCardinality implements Distance<List<AirQualityEvent>> {

    private final int k;
    private final Map<String, Double> inverseStds;
    private final KDTree originalTree;

    private static final String[] ATTRIBUTES = {
            "CO(GT)", "PT08.S1(CO)", "NMHC(GT)", "C6H6(GT)", "PT08.S2(NMHC)", "NOx(GT)",
            "PT08.S3(NOx)", "NO2(GT)", "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"
    };

    public KAnonymityPrivacyCardinality(List<AirQualityEvent> originalStream, int k) {
        if (k < 2) {
            throw new IllegalArgumentException("k must be at least 2");
        }
        this.k = k;

        // Calculate mean for each attribute in the original stream
        Map<String, Double> means = Arrays.stream(ATTRIBUTES).collect(Collectors.toMap(
                attr -> attr,
                attr -> originalStream.stream()
                        .mapToDouble(event -> OperatorUtils.getAttributeValue(event, attr))
                        .filter(v -> !Double.isNaN(v))
                        .average().orElse(0.0)
        ));

        // Calculate Standard Deviation for each attribute in the original stream
        this.inverseStds = Arrays.stream(ATTRIBUTES).collect(Collectors.toMap(
                attr -> attr,
                attr -> {
                    List<Double> values = originalStream.stream()
                            .map(event -> OperatorUtils.getAttributeValue(event, attr))
                            .filter(v -> !Double.isNaN(v))
                            .collect(Collectors.toList());

                    if (values.size() < 2) return 1.0;

                    double mean = means.get(attr);
                    double ssq = values.stream()
                            .mapToDouble(v -> (v - mean) * (v - mean))
                            .sum();
                    double std = Math.sqrt(ssq / (values.size() - 1));

                    // Store 1.0 / std to use multiplication later and avoid division for 0.0
                    return (std > 1e-9) ? 1.0 / std : 1.0;
                }
        ));

        // Build the tree once with the original stream
        // Skip tuples that are completely empty (all NaNs)
        List<double[]> originalVectors = originalStream.stream()
                .map(this::toVector)
                .filter(v -> !MetricUtils.isAllNaN(v))
                .collect(Collectors.toList());

        this.originalTree = new KDTree(originalVectors);
    }

    @Override
    public Double apply(List<AirQualityEvent> originalStream, List<AirQualityEvent> modifiedStream) {

        if (modifiedStream == null) {
            return 0.0;
        }

        // Calculate the Size Similarity Factor as g(r) = min(r, 1/r) where r is the size ratio
        double nOrig = originalStream.size();
        double nMod = modifiedStream.size();

        if (nOrig == 0) {
            return (nMod == 0) ? 1.0 : 0.0;
        }

        double sizeSimilarityFactor;
        if (nMod == 0) {
            // If the modified stream is empty, the size factor is 0. This will result in a final score of 0
            sizeSimilarityFactor = 0.0;
        } else {
            double sizeRatio = nMod / nOrig;
            sizeSimilarityFactor = Math.min(sizeRatio, nOrig / nMod);
        }

        // Early exit optimization: if the size penalty is already zero, the final score will be zero
        if (sizeSimilarityFactor == 0.0) {
            return 0.0;
        }

        // Calculate the Average Tuple Score
        double totalTupleScore = 0.0;
        int validTuplesCount = 0;

        // Iterate through every tuple in the modified stream
        for (AirQualityEvent modEvent : modifiedStream) {
            double[] targetVector = toVector(modEvent);

            // Skip if the tuple has no valid data
            if (MetricUtils.isAllNaN(targetVector)) continue;

            // Find the k nearest tuples in the original dataset
            List<Double> squaredDistances = originalTree.findNearestDistances(targetVector, k);
            if (squaredDistances.size() < 2) continue;

            // Convert squared distances to Euclidean distances
            List<Double> realDistances = new ArrayList<>();
            for (Double d2 : squaredDistances) {
                realDistances.add(Math.sqrt(d2));
            }

            // Calculate Standard Deviation of the Euclidean distances
            double stdDev = MetricUtils.calculateStdDev(realDistances);
            if (Double.isNaN(stdDev)) continue;

            // Calculate the individual score for this tuple (from 0 to 1)
            double tupleScore = 1.0 / (1.0 + stdDev);

            totalTupleScore += tupleScore;
            validTuplesCount++;
        }

        // If no valid tuples were found to score the quality is 0.
        if (validTuplesCount == 0) {
            return 0.0;
        }
        double avgTupleScore = totalTupleScore / validTuplesCount;

        // Calculate the Final Privacy Score
        double finalPrivacyScore = avgTupleScore * sizeSimilarityFactor;

        return finalPrivacyScore;
    }

    // Convert a tuple to a normalized double array
    private double[] toVector(AirQualityEvent e) {
        double[] v = new double[ATTRIBUTES.length];
        for (int i = 0; i < ATTRIBUTES.length; i++) {
            double val = OperatorUtils.getAttributeValue(e, ATTRIBUTES[i]);
            double inv = inverseStds.get(ATTRIBUTES[i]);
            v[i] = Double.isNaN(val) ? Double.NaN : val * inv;
        }
        return v;
    }

}