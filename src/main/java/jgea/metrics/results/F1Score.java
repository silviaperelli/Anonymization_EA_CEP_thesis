package jgea.metrics.results;

import event.GenericEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class F1Score implements Distance<List<GenericEvent>> {

    // Calculate the F1-score by comparing two sets of alert events, an event is a match if it has the same tuple ID
    @Override
    public Double apply(List<GenericEvent> groundTruth, List<GenericEvent> predictions) {
        if (groundTruth == null || predictions == null) {
            return 0.0;
        }

        // Extract the unique tuple ID from both lists
        Set<Long> groundTruthIds = groundTruth.stream()
                .map(event -> event.getAttribute("ID").longValue())
                .collect(Collectors.toSet());

        Set<Long> predictionIds = predictions.stream()
                .map(event -> event.getAttribute("ID").longValue())
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
