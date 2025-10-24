package jgea.problem;

import event.AirQualityEvent;
import event.StreamFactory;
import io.github.ericmedvet.jgea.core.distance.Distance;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import jgea.mappers.QueryRepresentation;
import jgea.mappers.RepresentationToLiebreQuery;
import jgea.metrics.EuclideanDistance;
import jgea.query.MainQuery;
import jgea.metrics.F1Score;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

// Define the multi-objective optimization problem
public class StreamAnonymizationProblem implements SimpleMOProblem<QueryRepresentation, Double> {

    // Define a static counter for unique query ID
    private static final AtomicLong queryCounter = new AtomicLong(0);

    // Define the objective for the multi-objective optimization
    private final static SequencedMap<String, Comparator<Double>> OBJECTIVES = new TreeMap<>(
            Map.ofEntries(
                    //Map.entry("privacy", ((Comparator<Double>) Double::compareTo).reversed()),
                    Map.entry("results-similarity", ((Comparator<Double>) Double::compareTo).reversed()),
                    Map.entry("metrics-difference", Double::compareTo)
            ));


    private final static Distance<List<AirQualityEvent>> RESULTS_SIMILARITY = new F1Score();
    private final static Distance<MainQuery.PerformanceMetrics> METRICS_DIFFERENCE = new EuclideanDistance();
    private final String inputCsvPath;
    private final List<AirQualityEvent> originalResults; // Ground truth results, calculated once in the constructor
    private final MainQuery.PerformanceMetrics originalMetrics;

    public StreamAnonymizationProblem(String inputCsvPath) throws Exception {
        this.inputCsvPath = inputCsvPath;

        // Load the original stream of events from the CSV file
        List<AirQualityEvent> originalStream = StreamFactory.createListFromFile(inputCsvPath);

        // Execute the main query
        MainQuery.QueryResult baselineOutcome = MainQuery.process(originalStream, "original");

        this.originalResults = baselineOutcome.events();
        this.originalMetrics = baselineOutcome.metrics();

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
            String queryId = String.valueOf(queryCounter.getAndIncrement());
            try {
                // Create an executable Liebre query and execute this anonymization query
                RepresentationToLiebreQuery liebreExecutor = new RepresentationToLiebreQuery();
                List<AirQualityEvent> modifiedEvents = liebreExecutor.processAnonymizationQuery(intermediateRepr, this.inputCsvPath);

                System.out.printf("[DEBUG][%s] Anonimizzazione terminata. Eventi nel dataset: %d%n", queryId, modifiedEvents.size());

                // If the modified datastream is empty, return 0 as F1 score and maximum difference
                if (modifiedEvents.isEmpty()) {
                    qualities.put("results-similarity", 0.0);
                    qualities.put("metrics-difference", Double.MAX_VALUE);
                    return qualities;
                }

                // Execute the main query
                MainQuery.QueryResult modifiedOutcome = MainQuery.process(modifiedEvents, String.valueOf(queryId));
                System.out.printf("[DEBUG][%s] Main Query terminata. Sequenze di allerta trovate: %d%n", queryId, modifiedOutcome.events().size());
                System.out.println("[DEBUG][%s] "+ queryId + " " + modifiedOutcome.metrics());

                // Populate the results map with F1 score and Euclidean distance
                qualities.put("results-similarity", RESULTS_SIMILARITY.apply(originalResults, modifiedOutcome.events()));
                qualities.put("metrics-difference", METRICS_DIFFERENCE.apply(originalMetrics, modifiedOutcome.metrics()));
                return qualities;

            } catch (Exception e) {
                System.err.printf("Error during fitness evaluation: %s", e.getMessage());
                e.printStackTrace();
                qualities.put("results-similarity", 0.0);
                qualities.put("metrics-difference", Double.MAX_VALUE);
                return qualities;
            }
        };
    }
}
