package jgea.problem;

import event.AirQualityEvent;
import event.StreamFactory;
import io.github.ericmedvet.jgea.core.distance.Distance;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import jgea.mappers.QueryRepresentation;
import jgea.mappers.RepresentationToLiebreQuery;
import jgea.query.MainQuery;
import jgea.metrics.F1Score;
import java.util.*;
import java.util.function.Function;

// Define the multi-objective optimization problem
public class StreamAnonymizationProblem implements SimpleMOProblem<QueryRepresentation, Double> {

    // Define the objective for the multi-objective optimization
    private final static SequencedMap<String, Comparator<Double>> OBJECTIVES = new TreeMap<>(
            Map.ofEntries(
                    //Map.entry("privacy", ((Comparator<Double>) Double::compareTo).reversed()),
                    Map.entry("results-similarity", ((Comparator<Double>) Double::compareTo).reversed())
                    //Map.entry("metrics-difference", Double::compareTo)
            ));


    private final static Distance<List<AirQualityEvent>> RESULTS_SIMILARITY = new F1Score();
    private final String inputCsvPath;
    private final List<AirQualityEvent> originalResults; // Ground truth results, calculated once in the constructor

    public StreamAnonymizationProblem(String inputCsvPath) throws Exception {
        this.inputCsvPath = inputCsvPath;

        // Load the original stream of events from the CSV file
        List<AirQualityEvent> originalStream = StreamFactory.createListFromFile(inputCsvPath);

        // Execute the main query
        this.originalResults = MainQuery.process(originalStream);

        System.out.println("Ground Truth generata");

    }

    @Override
    public SequencedMap<String, Comparator<Double>> comparators() {
        return OBJECTIVES;
    }


    @Override
    public Function<QueryRepresentation, SequencedMap<String, Double>> qualityFunction() {
        return intermediateRepr -> {
            // Build the results map
            SequencedMap<String, Double> qualities = new TreeMap<>();
            try {
                // Create an executable Liebre query and execute this anonymization query
                RepresentationToLiebreQuery liebreExecutor = new RepresentationToLiebreQuery();
                List<AirQualityEvent> modifiedEvents = liebreExecutor.processAnonymizationQuery(intermediateRepr, this.inputCsvPath);

                // If the modified datastream is empty, return 0
                if (modifiedEvents.isEmpty()) {
                    qualities.put("results-similarity", 0.0);
                    return qualities;
                }

                // Execute main query on the modified datastream
                List<AirQualityEvent> resultEvents = MainQuery.process(modifiedEvents);

                // Populate the results map with F1 score
                qualities.put("results-similarity", RESULTS_SIMILARITY.apply(originalResults, resultEvents));
                return qualities;

            } catch (Exception e) {
                System.err.printf("Error during fitness evaluation: %s", e.getMessage());
                e.printStackTrace();
                qualities.put("results-similarity", 0.0);
                return qualities;
            }
        };
    }
}
