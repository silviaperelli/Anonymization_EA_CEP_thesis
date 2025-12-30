package jgea.metrics.privacy;

import event.AirQualityEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;
import jgea.metrics.privacy.utils.KDTree;
import jgea.metrics.privacy.utils.MetricUtils;
import jgea.query.utils.OperatorUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Metric to evaluate the privacy of a modified stream
 *
 * Logic:
 * For each tuple in the modified stream, find its k tuples in the original stream
 * with minimun distance from that one. Calculate the Standard Deviation of distances to these k tuples.
 *
 * Lower StdDev -> Higher Privacy Score (Distance distribution is flat)
 */
public class KAnonymityPrivacy implements Distance<List<AirQualityEvent>> {

    private final int k;
    private final Map<String, Double> inverseStds;
    private final KDTree originalTree;

    private static final String[] ATTRIBUTES = {
            "CO(GT)", "PT08.S1(CO)", "NMHC(GT)", "C6H6(GT)", "PT08.S2(NMHC)", "NOx(GT)",
            "PT08.S3(NOx)", "NO2(GT)", "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"
    };

    public KAnonymityPrivacy(List<AirQualityEvent> originalStream, int k) {
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
        if (modifiedStream == null || modifiedStream.isEmpty()) return 1.0;

        double totalStdDev = 0.0;
        int count = 0;

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

            // Skip invalid results
            if (Double.isNaN(stdDev)) continue;

            totalStdDev += stdDev;
            count++;
        }

        if (count == 0) return 0.0;

        double averageStdDev = totalStdDev / count;

        // We want to maximize privacy
        // Formula: 1 / (1 + x) maps [0, inf) to [1, 0].
        return 1.0 / (1.0 + averageStdDev);
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