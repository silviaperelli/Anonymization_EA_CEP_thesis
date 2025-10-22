package jgea.utils;

import event.AirQualityEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class F1Score implements Distance<List<AirQualityEvent>> {

    // Calculate the F1-score by comparing two sets of alert events, an event is a match if it has the same tuple ID
    @Override
    public Double apply(List<AirQualityEvent> groundTruth, List<AirQualityEvent> predictions) {
        if (groundTruth == null || predictions == null) {
            return 0.0;
        }

        // Extract the unique tuple ID from both lists
        Set<Long> groundTruthIds = groundTruth.stream()
                .map(AirQualityEvent::getTupleId)
                .collect(Collectors.toSet());

        Set<Long> predictionIds = predictions.stream()
                .map(AirQualityEvent::getTupleId)
                .collect(Collectors.toSet());

        // Calculate True Positives (TP), ID that exist in both ground truth and predictions
        Set<Long> intersection = new HashSet<>(groundTruthIds);
        intersection.retainAll(predictionIds);
        int truePositive = intersection.size();

        // Calculate False Positives and False Negatives
        int falsePositive = predictionIds.size() - truePositive;
        int falseNegative = groundTruthIds.size() - truePositive;

        // Calculate Precision and Recall
        double precision = (truePositive + falsePositive > 0) ? (double) truePositive / (truePositive + falsePositive) : 0.0;
        double recall = (truePositive + falseNegative > 0) ? (double) truePositive / (truePositive + falseNegative) : 0.0;

        // Calculate F1 score
        if (precision + recall == 0) {
            return 0.0;
        }
        return 2 * (precision * recall) / (precision + recall);
    }
}
