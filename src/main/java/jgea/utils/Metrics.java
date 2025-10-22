package jgea.utils;

import evaluation.Sequence;
import event.AirQualityEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Metrics {

    
    public static double calculateF1Score(List<Sequence> groundTruth, List<Sequence> predictions) {
        int truePositive = 0;
        // Boolean array to mark predictions already matched to a ground truth sequence
        boolean[] matchedPredictions = new boolean[predictions.size()];

        // Search for an exact match in the prediction list
        for (Sequence truthSeq : groundTruth) {
            int bestMatchIndex = -1;
            for (int i = 0; i < predictions.size(); i++) {
                if (!matchedPredictions[i]) {
                    if (truthSeq.tupleIds().equals(predictions.get(i).tupleIds())) {
                        bestMatchIndex = i;
                        break;
                    }
                }
            }

            // If a match is found, increment the true positive counter and mark the prediction as used
            if (bestMatchIndex != -1) {
                truePositive++;
                matchedPredictions[bestMatchIndex] = true;
            }
        }

        int falseNegative = groundTruth.size() - truePositive;
        int falsePositive = predictions.size() - truePositive;

        double precision = (truePositive + falsePositive > 0) ? (double) truePositive / (truePositive + falsePositive) : 0;
        double recall = (truePositive + falseNegative > 0) ? (double) truePositive / (truePositive + falseNegative) : 0;

        if (precision + recall == 0) return 0.0;

        return 2 * (precision * recall) / (precision + recall);
    }

    // Calculate the F1-score by comparing two sets of alert events, an event is a match if it has the same tuple ID
    public static double calculateF1ScoreForEvents(List<AirQualityEvent> groundTruth, List<AirQualityEvent> predictions) {
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
