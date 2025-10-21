package jgea.newProblem;

import cep.PollutionAlertQuery;
import evaluation.Sequence;
import event.AirQualityEvent;
import event.StreamFactory;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import jgea.representation.QueryRepresentation;
import jgea.representation.RepresentationToLiebreQuery;
import jgea.utils.Metrics;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.Evaluator;

import java.util.*;
import java.util.function.Function;


public class StreamAnonymizationProblem implements SimpleMOProblem<QueryRepresentation, Double> {

    private final static SequencedMap<String, Comparator<Double>> OBJECTIVES = new TreeMap<>(
            Map.ofEntries(
                    //Map.entry("privacy", ((Comparator<Double>) Double::compareTo).reversed()),
                    Map.entry("results-similarity", ((Comparator<Double>) Double::compareTo).reversed())
                    //Map.entry("metrics-difference", Double::compareTo)
            ));


    private final String inputCsvPath;
    private final List<Sequence> originalResults; // Usiamo le Sequence per la ground truth

    public StreamAnonymizationProblem(String inputCsvPath) throws Exception {

        this.inputCsvPath = inputCsvPath;
        StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.createLocalEnvironment();
        // Create a Flink dataStream from the original CSV file
        DataStream<AirQualityEvent> originalStream = StreamFactory.createStreamfromFile(flinkEnv, inputCsvPath);

        // Execute the CEP query
        List<List<AirQualityEvent>> cepResultEvents = PollutionAlertQuery.processAlerts(originalStream);
        // Converts results from Flink's format into List<Sequence> format
        this.originalResults = Evaluator.parseSequencesFromEvents(cepResultEvents);

    }

    @Override
    public SequencedMap<String, Comparator<Double>> comparators() {
        return OBJECTIVES;
    }

    @Override
    public Function<QueryRepresentation, SequencedMap<String, Double>> qualityFunction() {
        return intermediateRepr -> {
            try {
                // Execute anonymization query
                RepresentationToLiebreQuery liebreExecutor = new RepresentationToLiebreQuery();
                List<AirQualityEvent> modifiedEvents = liebreExecutor.processAnonymizationQuery(intermediateRepr, this.inputCsvPath);

                // Costruisci la mappa dei risultati
                SequencedMap<String, Double> qualities = new TreeMap<>();

                // If the modified datastream is empty, return 0
                if (modifiedEvents.isEmpty()) {
                    qualities.put("results-similarity", 0.0);
                    return qualities;
                }

                // Execute query cep on the modified datastream
                StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.createLocalEnvironment();
                DataStream<AirQualityEvent> modifiedStream = event.StreamFactory.createStreamFromCollection(flinkEnv, modifiedEvents);
                List<List<AirQualityEvent>> cepResultEvents = PollutionAlertQuery.processAlerts(modifiedStream);

                // Compare the sequences found in the modified data with the original sequences (ground truth) to calculate the F1 score
                List<Sequence> modifiedResults = Evaluator.parseSequencesFromEvents(cepResultEvents);

                // 4. Calcola l'F1-Score
                double f1Score = Metrics.calculateF1Score(originalResults, modifiedResults);

                // 5. Popola la mappa dei risultati
                qualities.put("results-similarity", f1Score);

                return qualities;

            } catch (Exception e) {
                System.err.printf("Error during fitness evaluation: %s", e.getMessage());
                e.printStackTrace();
                SequencedMap<String, Double> qualities = new TreeMap<>();
                qualities.put("results-similarity", 0.0);
                return qualities;
            }
        };
    }
}
