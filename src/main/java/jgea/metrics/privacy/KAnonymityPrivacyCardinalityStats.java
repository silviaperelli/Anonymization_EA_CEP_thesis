package jgea.metrics.privacy;

import event.GenericEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;
import jgea.metrics.privacy.utils.KDTree;
import jgea.metrics.privacy.utils.MetricUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * KAnonymityPrivacyCardinalityStats implements a k-anonymity–based privacy metric.
 *
 * Privacy is evaluated by analyzing the dispersion (standard deviation)
 * of the distances to the k nearest neighbors in the original dataset.
 *
 * The metric also exposes descriptive statistics (mean, min, max, quantiles)
 * on the standard deviation values to support detailed analysis and plotting.
 */
public class KAnonymityPrivacyCardinalityStats
        implements Distance<List<GenericEvent>> {

    public static class StdDevStats {
        public double mean = 0.0;
        public double min = 0.0; // Best-case privacy (Minimum stddev)
        public double max = 0.0; // Worst-case privacy (Maximum stddev)
        public double q95 = 0.0;
        public double q99 = 0.0;
    }

    private final int k;
    private final Map<String, Double> inverseStds;
    private final KDTree originalTree;
    private final List<String> attributes;

    public KAnonymityPrivacyCardinalityStats(
            List<GenericEvent> originalStream, int k, List<String> attributes) {

        if (k < 2) {
            throw new IllegalArgumentException("k must be at least 2");
        }
        this.k = k;
        this.attributes = attributes;

        // Calculate mean for each attribute in the original stream
        Map<String, Double> means = this.attributes.stream()
                .collect(Collectors.toMap(
                        a -> a,
                        a -> originalStream.stream()
                                .mapToDouble(e -> e.getAttribute(a))
                                .filter(v -> !Double.isNaN(v))
                                .average()
                                .orElse(0.0)
                ));

        // Calculate Standard Deviation for each attribute in the original stream
        this.inverseStds = this.attributes.stream()
                .collect(Collectors.toMap(
                        a -> a,
                        a -> {
                            List<Double> values = originalStream.stream()
                                    .map(e -> e.getAttribute(a))
                                    .filter(v -> !Double.isNaN(v))
                                    .collect(Collectors.toList());
                            if (values.size() < 2) return 1.0;
                            double mean = means.get(a);
                            double ssq = values.stream()
                                    .mapToDouble(v -> (v - mean) * (v - mean))
                                    .sum();
                            double std = Math.sqrt(ssq / (values.size() - 1));
                            return (std > 1e-9) ? 1.0 / std : 1.0;
                        }
                ));

        // Build the tree once with the original stream
        List<double[]> originalVectors = originalStream.stream()
                .map(this::toVector)
                .filter(v -> !MetricUtils.isAllNaN(v))
                .collect(Collectors.toList());
        this.originalTree = new KDTree(originalVectors);
    }

    /**
     * Implementation of the Distance interface.
     * Return a single privacy score based on the mean standard deviation.
     */
    @Override
    public Double apply(List<GenericEvent> original,
                        List<GenericEvent> modified) {
        // Compute raw statistics on standard deviations
        StdDevStats stats = applyWithStdDevStats(original, modified);
        // Convert mean standard deviation (privacy risk) into a score to maximize
        return 1.0 / (1.0 + stats.mean);
    }

    /**
     * Computes statistics directly on the standard deviation of k-nearest-neighbor distances.
     */
    public StdDevStats applyWithStdDevStats(
            List<GenericEvent> original,
            List<GenericEvent> modified) {

        StdDevStats stats = new StdDevStats();

        if (original == null || modified == null ||
                original.isEmpty() || modified.isEmpty()) {
            return stats;
        }

        // Collect raw standard deviation values for each event
        List<Double> stddevs = new ArrayList<>();

        for (GenericEvent e : modified) {
            double[] v = toVector(e);
            if (MetricUtils.isAllNaN(v)) continue;

            // Retrieve squared distances of the k nearest neighbors
            List<Double> squaredDistances =
                    originalTree.findNearestDistances(v, k);

            if (squaredDistances.size() < 2) continue;

            // Convert squared distances to Euclidean distances
            List<Double> distances = new ArrayList<>();
            for (Double d2 : squaredDistances) {
                distances.add(Math.sqrt(d2));
            }

            // Compute standard deviation of distances
            double std = MetricUtils.calculateStdDev(distances);
            if (Double.isNaN(std)) continue;

            stddevs.add(std);
        }

        if (stddevs.isEmpty()) return stats;

        // Sort values to compute quantiles
        Collections.sort(stddevs);
        int n = stddevs.size();

        // Compute aggregate statistics
        stats.mean = stddevs.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);

        stats.min = stddevs.get(0);
        stats.max = stddevs.get(n - 1);

        // Calcolo corretto dei quantili
        stats.q95 = stddevs.get((int) Math.floor(0.95 * (n - 1)));
        stats.q99 = stddevs.get((int) Math.floor(0.99 * (n - 1)));

        return stats;
    }

    // Converts a GenericEvent into a normalized feature vector
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