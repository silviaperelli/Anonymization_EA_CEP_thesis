package jgea.metrics.privacy;

import event.GenericEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;
import jgea.metrics.privacy.utils.KDTree;
import jgea.metrics.privacy.utils.MetricUtils;

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
public class KAnonymityPrivacy implements Distance<List<GenericEvent>> {

    private final int k;
    private final Map<String, Double> inverseStds;
    private final KDTree originalTree;
    private final List<String> attributes;

    public KAnonymityPrivacy(List<GenericEvent> originalStream, int k, List<String> attributes) {
        if (k < 2) {
            throw new IllegalArgumentException("k must be at least 2");
        }
        this.k = k;
        this.attributes = attributes;

        // Calculate mean for each attribute in the original stream
        Map<String, Double> means = this.attributes.stream().collect(Collectors.toMap(
                attr -> attr,
                attr -> originalStream.stream()
                        .mapToDouble(event -> event.getAttribute(attr))
                        .filter(v -> !Double.isNaN(v))
                        .average().orElse(0.0)
        ));

        // Calculate Standard Deviation for each attribute in the original stream
        this.inverseStds = this.attributes.stream().collect(Collectors.toMap(
                attr -> attr,
                attr -> {
                    List<Double> values = originalStream.stream()
                            .map(event -> event.getAttribute(attr))
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
    public Double apply(List<GenericEvent> originalStream, List<GenericEvent> modifiedStream) {
        if (modifiedStream == null || modifiedStream.isEmpty()) return 1.0;

        double totalStdDev = 0.0;
        int count = 0;

        // Iterate through every tuple in the modified stream
        for (GenericEvent modEvent : modifiedStream) {
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
    private double[] toVector(GenericEvent e) {
        double[] v = new double[this.attributes.size()];
        for (int i = 0; i < this.attributes.size(); i++) {
            String attrName = this.attributes.get(i);
            double val = e.getAttribute(attrName);
            double inv = inverseStds.get(attrName);
            v[i] = Double.isNaN(val) ? Double.NaN : val * inv;
        }
        return v;
    }
}