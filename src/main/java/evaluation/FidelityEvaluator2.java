package evaluation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import static utils.Evaluator.parseSequencesFromFile;

// Evaluation made with Overlapping Coefficient
public class FidelityEvaluator2 {

    private static final Logger logger = LoggerFactory.getLogger(FidelityEvaluator2.class);
    // Threshold to define a match (the intersection is at least 50% of the min size of the sequences)
    private static final double OVERLAP_THRESHOLD = 0.5;

    public static void main(String[] args) {
        // Modifica questi percorsi per puntare ai tuoi file CSV
        Path originalFilePath = Paths.get("src/main/resources/datasets/target/targetDataset.csv");

        //Path anonymizedFilePath = Paths.get("src/main/resources/datasets/target/targetAnonymizedDataset.csv");
        Path anonymizedFilePath = Paths.get("src/main/resources/datasets/target/targetAnonymizedDatasetNoise.csv");

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
        // Find the intersection in the two sequences
        LocalDateTime intersectionStart = s1.startTime().isAfter(s2.startTime()) ? s1.startTime() : s2.startTime();
        LocalDateTime intersectionEnd = s1.endTime().isBefore(s2.endTime()) ? s1.endTime() : s2.endTime();

        // Calculate the duration of the intersection
        long intersectionDuration = 0;
        if (intersectionEnd.isAfter(intersectionStart)) {
            intersectionDuration = Duration.between(intersectionStart, intersectionEnd).toSeconds();
        }

        if (intersectionDuration == 0) {
            return 0.0;
        }

        // Calculate the minimum duration between the two sequences
        long duration1 = Duration.between(s1.startTime(), s1.endTime()).toSeconds();
        long duration2 = Duration.between(s2.startTime(), s2.endTime()).toSeconds();
        long minDuration = Math.min(duration1, duration2);

        if (minDuration == 0) {
            return 0.0;
        }

        return (double) intersectionDuration / minDuration;
    }
}
