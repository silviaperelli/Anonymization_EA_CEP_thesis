package jgea.problem;

import event.AirQualityEvent;
import event.StreamFactory;
import io.github.ericmedvet.jgea.core.distance.Distance;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import jgea.mappers.QueryRepresentation;
import jgea.query.LiebreAnonymizationQuery;
import jgea.metrics.EuclideanDistance;
import jgea.metrics.PrivacyScore;
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
                    Map.entry("privacy", ((Comparator<Double>) Double::compareTo).reversed()), // Maximize
                    Map.entry("results-similarity", ((Comparator<Double>) Double::compareTo).reversed()), // Maximize
                    Map.entry("metrics-difference", Double::compareTo)  // Minimize
            ));


    private final static Distance<List<AirQualityEvent>> RESULTS_SIMILARITY = new F1Score();
    private final static Distance<MainQuery.PerformanceMetrics> METRICS_DIFFERENCE = new EuclideanDistance();
    private final static Distance<List<AirQualityEvent>> PRIVACY = new PrivacyScore();

    private final String inputCsvPath;
    private final List<AirQualityEvent> originalStream;
    private final List<AirQualityEvent> originalResults; // Ground truth results, calculated once in the constructor
    private final MainQuery.PerformanceMetrics originalMetrics;

    public StreamAnonymizationProblem(String inputCsvPath) throws Exception {
        this.inputCsvPath = inputCsvPath;

        // Load the original stream of events from the CSV file
        this.originalStream = StreamFactory.createListFromFile(inputCsvPath);

        // Execute the main query
        MainQuery.QueryResult baselineOutcome = MainQuery.process(this.originalStream, "original");

        System.out.printf("[DEBUG][Main Query Metrics] " + baselineOutcome.metrics() + "%n");

        this.originalResults = baselineOutcome.events();
        this.originalMetrics = baselineOutcome.metrics();

        System.out.println("Ground Truth generated");

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
            Long counter = queryCounter.getAndIncrement();
            String queryId = String.valueOf(counter);
            try {
                // Create an executable Liebre query and execute this anonymization query
                LiebreAnonymizationQuery liebreExecutor = new LiebreAnonymizationQuery();
                List<AirQualityEvent> modifiedEvents = liebreExecutor.processAnonymizationQuery(intermediateRepr, this.inputCsvPath);

                // If the modified datastream is empty, return 0 as F1 score and maximum difference
                if (modifiedEvents.isEmpty()) {
                    qualities.put("results-similarity", 0.0);
                    qualities.put("metrics-difference", Double.MAX_VALUE);
                    qualities.put("privacy", 1.0);
                    if (counter % 50 == 0) {
                        System.out.printf(
                                "Evaluation %5d -> Result: Empty stream. Assigning worst-case fitness.%n",
                                counter
                        );
                    }
                    return qualities;
                }

                // Execute the main query
                MainQuery.QueryResult modifiedOutcome = MainQuery.process(modifiedEvents, String.valueOf(queryId));

                // Populate the results map with F1 score, Euclidean distance and privacy score
                qualities.put("results-similarity", RESULTS_SIMILARITY.apply(originalResults, modifiedOutcome.events()));
                qualities.put("metrics-difference", METRICS_DIFFERENCE.apply(originalMetrics, modifiedOutcome.metrics()));
                qualities.put("privacy", PRIVACY.apply(this.originalStream, modifiedEvents));

                if (modifiedOutcome.metrics().afterAggregate() == 0 || modifiedOutcome.metrics().afterFilter1() == 0 || modifiedOutcome.metrics().afterFilter2() == 0 || modifiedOutcome.metrics().beforeFilter1() == 0
                        || modifiedOutcome.metrics().beforeAggregate() == 0 || modifiedOutcome.metrics().beforeFilter2() == 0 || modifiedOutcome.metrics().afterSource() == 0 || modifiedOutcome.metrics().beforeSink() == 0){
                    System.out.printf("[DEBUG][%s] " + modifiedOutcome.metrics() + "%n", queryId);
                    System.out.printf("[DEBUG][%s] " + intermediateRepr + "%n", queryId);
                    System.out.printf("[DEBUG][%s] Tuples in the anonymization dataset: %d%n", queryId, modifiedEvents.size());
                    System.out.printf("[DEBUG][%s] Alert tuples found by the main query: %d%n", queryId, modifiedOutcome.events().size());
                    System.out.printf("[DEBUG][%s] Metrics difference: %.3e%n", queryId, qualities.get("metrics-difference"));
                }

                if (counter % 50 == 0) {
                    System.out.printf(
                            "Evaluation %5d -> Privacy: %.3f | Similarity: %.3f | Diff: %.3e%n",
                            counter,
                            qualities.get("privacy"),
                            qualities.get("results-similarity"),
                            qualities.get("metrics-difference")
                    );
                }

                return qualities;

            } catch (Exception e) {
                System.err.printf("Error during fitness evaluation: %s", e.getMessage());
                e.printStackTrace();
                qualities.put("results-similarity", 0.0);
                qualities.put("metrics-difference", Double.MAX_VALUE);
                qualities.put("privacy", 1.0);
                return qualities;
            }
        };
    }
}