package jgea;

import cep.PollutionAlertQuery;
import evaluation.Sequence;
import event.AirQualityEvent;
import event.StreamFactory;
import io.github.ericmedvet.jgea.core.problem.TotalOrderQualityBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.GrammarBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import jgea.representation.QueryRepresentation;
import jgea.representation.RepresentationToLiebreQuery;
import jgea.representation.TreeToRepresentation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.Evaluator;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class Problem implements GrammarBasedProblem<String, QueryRepresentation>,
        TotalOrderQualityBasedProblem<QueryRepresentation, Double> {

    private final StringGrammar<String> grammar;
    private final String inputCsvPath;
    private final List<Sequence> originalCepResults;

    // Constructor that process the original dataset with the query cep to generate ground truth
    public Problem(String grammarPath, String inputCsvPath) throws IOException {
        this.inputCsvPath = inputCsvPath;
        try (FileInputStream fis = new FileInputStream(grammarPath)) {
            this.grammar = StringGrammar.load(fis);
        }
        System.out.println("Processing original dataset with CEP to generate ground truth...");

        try {
            StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.createLocalEnvironment();

            // Create a Flink dataStream from the original CSV file
            DataStream<AirQualityEvent> originalStream = StreamFactory.createStreamfromFile(flinkEnv, inputCsvPath);

            // Execute the CEP query
            List<List<AirQualityEvent>> cepResultEvents = PollutionAlertQuery.processAlerts(originalStream);

            // Converts results from Flink's format into List<Sequence> format
            this.originalCepResults = Evaluator.parseSequencesFromEvents(cepResultEvents);

            System.out.println("Ground truth generated successfully. Found " + this.originalCepResults.size() + " original sequences.");

        } catch (Exception e) {
            System.err.println("Error while executing the CEP query on original dataset.");
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize ground truth CEP results", e);
        }
    }

    @Override
    public StringGrammar<String> grammar() {
        return grammar;
    }

    @Override
    public Function<Tree<String>, QueryRepresentation> solutionMapper() {
        return (Tree<String> tree) -> {
            try{
                // First mapping from tree to PipelineRepresentation
                List<QueryRepresentation.OperatorNode> operators = new ArrayList<>();
                // Start recursive parsing from the root of the tree
                TreeToRepresentation firstMapper = new TreeToRepresentation();
                firstMapper.parsePipelineNode(tree, operators);

                return new QueryRepresentation(operators);
            }catch(Exception e){
                System.err.println(("Error during mapping process"));
                return null;
            }
        };
    }

    @Override
    public Comparator<Double> totalOrderComparator() {
        return Comparator.reverseOrder(); // Maximize fitness
    }

    @Override
    public Function<QueryRepresentation, Double> qualityFunction() {
        return intermediateRepr -> {
            try {
                // Execute anonymization query
                RepresentationToLiebreQuery liebreExecutor = new RepresentationToLiebreQuery();
                List<AirQualityEvent> modifiedEvents = liebreExecutor.processAnonymizationQuery(intermediateRepr, this.inputCsvPath);

                // If the modified datastream is empty, return 0
                if (modifiedEvents.isEmpty()) {
                  return 0.0;
                }

                // Execute query cep on the modified datastream
                StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.createLocalEnvironment();
                DataStream<AirQualityEvent> modifiedStream = event.StreamFactory.createStream(flinkEnv, modifiedEvents);
                List<List<AirQualityEvent>> cepResultEvents = PollutionAlertQuery.processAlerts(modifiedStream);

                // Compare the sequences found in the modified data with the original sequences (ground truth) to calculate the F1 score
                List<Sequence> modifiedCepResults = Evaluator.parseSequencesFromEvents(cepResultEvents);

                return calculateF1Score(originalCepResults, modifiedCepResults);

            } catch (Exception e) {
                System.err.printf("Error during fitness evaluation: %s", e.getMessage());
                e.printStackTrace();
                return 0.0;
            }
        };

    }


    private double calculateF1Score(List<Sequence> groundTruth, List<Sequence> predictions) {
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
