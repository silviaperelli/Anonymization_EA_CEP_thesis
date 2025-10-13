package evaluation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import static utils.Evaluator.parseSequencesFromFile;

// Evaluation made with Overlapping Coefficient
public class FidelityEvaluator2 {

    private static final Logger logger = LoggerFactory.getLogger(FidelityEvaluator2.class);
    // Threshold to define a match (the intersection is at least 50% of the min size of the sequences)
    private static final double OVERLAP_THRESHOLD = 0.5;

    public static void main(String[] args) {

        Path originalFilePath = Paths.get("src/main/resources/datasets/results/targetDataset.csv");
        Path anonymizedFilePath = Paths.get("src/main/resources/datasets/results/targetAnonymizedDataset.csv");
        //Path anonymizedFilePath = Paths.get("src/main/resources/datasets/results/targetAnonymizedDatasetNoise.csv");

        try {
            List<Sequence> originalSequences = parseSequencesFromFile(originalFilePath);
            List<Sequence> anonymizedSequences = parseSequencesFromFile(anonymizedFilePath);
            evaluate(originalSequences, anonymizedSequences);
        } catch (IOException e) {
            logger.error("Error reading files: ", e);
        }
    }

    // Execute the matching algorithm and calculate the fidelity metrics
    public static void evaluate(List<Sequence> groundTruth, List<Sequence> predictions) {

        int truePositive = 0;

        // Array used to keep track of the predicted sequence that have already been matched
        boolean[] matchedPredictions = new boolean[predictions.size()];

        // Cycle through each ground truth sequence
        for (Sequence truthSeq : groundTruth){

            double bestIoU = -1.0;
            int bestMatchIndex = -1;

            // Evaluate the best match in the predicted sequence (anonymized)
            for (int i = 0; i < predictions.size(); i++){
                if (!matchedPredictions[i]) {
                    double iou = calculateOverlapCoefficient(truthSeq, predictions.get(i));
                    if (iou > bestIoU){
                        bestIoU = iou;
                        bestMatchIndex = i;
                    }
                }
            }

            if (bestIoU >= OVERLAP_THRESHOLD){
                truePositive++;
                matchedPredictions[bestMatchIndex] = true;
            }
        }

        int falseNegative = groundTruth.size() - truePositive; // Real sequence not predicted
        int falsePositive = predictions.size() - truePositive; // Predicted sequence that doesn't correspond to any real one

        double precision = (truePositive + falsePositive > 0) ? (double) truePositive / (truePositive + falsePositive) : 0;
        double recall = (truePositive + falseNegative > 0) ? (double) truePositive / (truePositive + falseNegative) : 0;
        double f1Score = (precision + recall > 0) ? 2 * (precision * recall) / (precision + recall) : 0;

        logger.info("--- Evaluation results with Overlapping Coefficient ---");
        logger.info("True Positives (TP): " + truePositive);
        logger.info("False Positives (FP): " + falsePositive);
        logger.info("False Negatives (FN): " + falseNegative);
        logger.info("------------------------------------");
        logger.info(String.format("Precision: %.2f%%", precision * 100));
        logger.info(String.format("Recall:    %.2f%%", recall * 100));
        logger.info(String.format("F1-Score:  %.2f%%", f1Score * 100));
        logger.info("------------------------------------");
    }

    // Calculate Overlap Coefficiente between two sequences
    private static double calculateOverlapCoefficient(Sequence s1, Sequence s2) {
        Set<Long> intersection = s1.intersection(s2);

        if (intersection.isEmpty()) {
            return 0.0;
        }

        // The denominator is the size of the smallest sequence
        int minSize = Math.min(s1.tupleIds().size(), s2.tupleIds().size());

        if (minSize == 0) {
            return 0.0;
        }

        return (double) intersection.size() / minSize;
    }
}
