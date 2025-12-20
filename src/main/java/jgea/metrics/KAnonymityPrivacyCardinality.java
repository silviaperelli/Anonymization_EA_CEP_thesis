package jgea.metrics;

import event.AirQualityEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;
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
                .filter(v -> !isAllNaN(v))
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
            if (isAllNaN(targetVector)) continue;

            // Find the k nearest tuples in the original dataset
            List<Double> squaredDistances = originalTree.findNearestDistances(targetVector, k);
            if (squaredDistances.size() < 2) continue;

            // Convert squared distances to Euclidean distances
            List<Double> realDistances = new ArrayList<>();
            for (Double d2 : squaredDistances) {
                realDistances.add(Math.sqrt(d2));
            }

            // Calculate Standard Deviation of the Euclidean distances
            double stdDev = calculateStdDev(realDistances);
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

    // Helper to check if a vector contains only NaN values
    private boolean isAllNaN(double[] vector) {
        for (double v : vector) {
            if (!Double.isNaN(v)) return false;
        }
        return true;
    }

    // Standard Deviation calculation
    private double calculateStdDev(List<Double> distances) {
        double sum = 0.0;
        for (double d : distances) sum += d;
        double mean = sum / distances.size();

        double sqDiff = 0.0;
        for (double d : distances) {
            double diff = d - mean;
            sqDiff += diff * diff;
        }
        return Math.sqrt(Math.max(0.0, sqDiff / (distances.size() - 1)));
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

    // KDTree Implementation
    private static class KDTree {
        private static class Node {
            double[] point;
            int axis;
            Node left, right;
            boolean rightHasNaNs; // Flag to know if NaNs exist in the right branch to help pruning
            Node(double[] point, int axis) { this.point = point; this.axis = axis; }
        }

        private final Node root;
        private final int dims;
        private final double maxDims;

        KDTree(List<double[]> points) {
            if (points.isEmpty()) {
                this.root = null;
                this.dims = 0;
                this.maxDims = 0;
            } else {
                this.dims = points.get(0).length;
                this.maxDims = (double) this.dims;
                this.root = build(points, 0);
            }
        }

        // Recursive build function
        private Node build(List<double[]> pts, int depth) {
            if (pts.isEmpty()) return null;
            int axis = depth % dims;

            // Sort points based on current axis
            pts.sort((a, b) -> {
                double va = a[axis];
                double vb = b[axis];
                if (Double.isNaN(va) && Double.isNaN(vb)) return 0;
                if (Double.isNaN(va)) return 1; // Send NaNs at the end (right)
                if (Double.isNaN(vb)) return -1;
                return Double.compare(va, vb);
            });
            int mid = pts.size() / 2;
            Node node = new Node(pts.get(mid), axis);

            // Check if the right subset contains NaNs to set the flag
            boolean hasNaNs = false;
            if (pts.size() > mid + 1) {
                double lastVal = pts.get(pts.size() - 1)[axis];
                if (Double.isNaN(lastVal)) hasNaNs = true;
            }
            node.rightHasNaNs = hasNaNs;
            node.left = build(pts.subList(0, mid), depth + 1);
            node.right = build(pts.subList(mid + 1, pts.size()), depth + 1);
            return node;
        }

        // Method to find distances of k nearest tuples
        List<Double> findNearestDistances(double[] target, int k) {
            // Priority Queue to keep track of the k smallest distances so far
            // The head of the queue is the largest distance among the k best
            PriorityQueue<Double> pq = new PriorityQueue<>(k, Collections.reverseOrder());
            searchKNearest(root, target, k, pq);
            return new ArrayList<>(pq);
        }

        // Search for the k nearest tuples with pruning
        private void searchKNearest(Node node, double[] target, int k, PriorityQueue<Double> pq) {
            if (node == null) return;

            // Calculate distance between target and current node
            double distSq = calculateMeanDistance(target, node.point);

            // Add to queue if valid
            if (!Double.isNaN(distSq) && !Double.isInfinite(distSq)) {
                if (pq.size() < k) {
                    pq.add(distSq);
                } else if (distSq < pq.peek()) {
                    pq.poll(); // Remove worst of the best
                    pq.add(distSq); // Add new candidate
                }
            }

            // Determine which child to visit first
            int axis = node.axis;
            double targetVal = target[axis];
            double nodeVal = node.point[axis];

            // If we have NaNs on the splitting axis, we cannot make a binary decision so we visit both
            if (Double.isNaN(targetVal) || Double.isNaN(nodeVal)) {
                searchKNearest(node.left, target, k, pq);
                searchKNearest(node.right, target, k, pq);
                return;
            }

            double diff = targetVal - nodeVal;
            double diffSq = diff * diff;
            Node near = diff < 0 ? node.left : node.right;
            Node far = diff < 0 ? node.right : node.left;

            // Visit the neared side
            searchKNearest(near, target, k, pq);

            // Pruning logic
            boolean mustVisitFar = false;
            if (pq.size() < k)
                // If we haven't found k tuples yet, we must search everywhere
                mustVisitFar = true;
            else if (diffSq < (pq.peek() * maxDims))
                mustVisitFar = true;
            if (!mustVisitFar && far == node.right && node.rightHasNaNs) mustVisitFar = true;
            if (mustVisitFar) searchKNearest(far, target, k, pq);
        }

        // Calculate Mean Squared Distance
        private static double calculateMeanDistance(double[] a, double[] b) {
            double sum = 0;
            int valid = 0;
            for (int i = 0; i < a.length; i++) {
                if (!Double.isNaN(a[i]) && !Double.isNaN(b[i])) {
                    double d = a[i] - b[i];
                    sum += d * d;
                    valid++;
                }
            }
            if (valid == 0) return Double.POSITIVE_INFINITY;
            return sum / valid;
        }
    }
}