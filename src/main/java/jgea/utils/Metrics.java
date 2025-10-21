package jgea.utils;

import evaluation.Sequence;

import java.util.List;

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

}
