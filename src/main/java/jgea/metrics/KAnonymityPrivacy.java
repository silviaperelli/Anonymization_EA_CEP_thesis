package jgea.metrics;

import event.AirQualityEvent;
import jgea.query.utils.OperatorUtils;
import io.github.ericmedvet.jgea.core.distance.Distance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KAnonymityPrivacy implements Distance<List<AirQualityEvent>> {

    private final int k;
    private final double epsilonSquared;
    private final Map<String, Double> inverseStds;

    private static final String[] ATTRIBUTES = {
            "CO(GT)", "PT08.S1(CO)", "NMHC(GT)", "C6H6(GT)", "PT08.S2(NMHC)", "NOx(GT)",
            "PT08.S3(NOx)", "NO2(GT)", "PT08.S4(NO2)", "PT08.S5(O3)", "T", "RH", "AH"
    };

    public KAnonymityPrivacy(List<AirQualityEvent> originalStream, int k, double epsilon) {
        if (k <= 0) {
            throw new IllegalArgumentException("k must be positive.");
        }
        this.k = k;
        this.epsilonSquared = epsilon * epsilon;

        Map<String, Double> means = java.util.Arrays.stream(ATTRIBUTES).collect(Collectors.toMap(
                attr -> attr,
                attr -> originalStream.stream()
                        .mapToDouble(event -> OperatorUtils.getAttributeValue(event, attr))
                        .filter(value -> !Double.isNaN(value))
                        .average()
                        .orElse(0.0)
        ));

        this.inverseStds = java.util.Arrays.stream(ATTRIBUTES).collect(Collectors.toMap(
                attr -> attr,
                attr -> {
                    List<Double> values = originalStream.stream()
                            .map(event -> OperatorUtils.getAttributeValue(event, attr))
                            .filter(value -> !Double.isNaN(value))
                            .collect(Collectors.toList());

                    if (values.size() < 2) {
                        return 1.0;
                    }

                    double mean = means.get(attr);
                    double sumOfSquares = values.stream()
                            .mapToDouble(value -> Math.pow(value - mean, 2))
                            .sum();

                    double stdDev = Math.sqrt(sumOfSquares / (values.size() - 1));

                    return (stdDev > 1e-9) ? (1.0 / stdDev) : 1.0;
                }
        ));

        System.out.println("KAnonymityPrivacy metric initialized with k=" + k + " and epsilon=" + epsilon);
    }

    @Override
    public Double apply(List<AirQualityEvent> originalStream, List<AirQualityEvent> modifiedStream) {
        if (modifiedStream == null || modifiedStream.isEmpty()) {
            return 0.0;
        }

        List<Double> tupleScores = new ArrayList<>();

        for (AirQualityEvent r1 : modifiedStream) {
            int equivalenceClassSize = 0;
            for (AirQualityEvent r2 : modifiedStream) {
                double dist = averageDistanceSquared(r1, r2);


                if (dist < this.epsilonSquared) {
                    equivalenceClassSize++;
                    if (equivalenceClassSize >= k) break;
                }
            }
            double score = Math.min((double) equivalenceClassSize / this.k, 1.0);
            tupleScores.add(score);
        }

        return tupleScores.stream().mapToDouble(d -> d).average().orElse(0.0);
    }

    //Calculates the square of the normalized Euclidean distance between two tuples,
    //normalized by the number of valid dimensions.
    private double averageDistanceSquared(AirQualityEvent r1, AirQualityEvent r2) {
        double totalSquaredDistance = 0.0;
        int validDimensions = 0;

        for (String attr : ATTRIBUTES) {
            double val1 = OperatorUtils.getAttributeValue(r1, attr);
            double val2 = OperatorUtils.getAttributeValue(r2, attr);

            if (!Double.isNaN(val1) && !Double.isNaN(val2)) {
                double invStd = this.inverseStds.get(attr);

                // The complete normalization would be (val - mean) * invStd, but the mean cancels out in the difference.
                double diff = (val1 - val2) * invStd;
                totalSquaredDistance += diff * diff;
                validDimensions++;
            }
        }

        if (validDimensions == 0) {
            return Double.POSITIVE_INFINITY;
        }

        // Returns the mean square distance for dimension
        return totalSquaredDistance / validDimensions;
    }
}

