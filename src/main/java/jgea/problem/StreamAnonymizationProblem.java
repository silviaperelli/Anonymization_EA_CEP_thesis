package jgea.problem;

import common.metrics.Metrics;
import event.AirQualityEvent;
import event.StreamFactory;
import io.github.ericmedvet.jgea.core.distance.Distance;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import jgea.mappers.QueryRepresentation;
import jgea.metrics.*;
import jgea.query.LiebreAnonymizationQuery;
import jgea.query.MainQuery;
import query.LiebreContext;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

// Define the multi-objective optimization problem
public class StreamAnonymizationProblem implements SimpleMOProblem<QueryRepresentation, Double> {

    // Define a static counter for unique query ID
    private static final AtomicLong queryCounter = new AtomicLong(0);

    static {
        //Since metrics will be added and removed during the query execution, we need to
        //ensure that the LiebreContext is initialized with the right metrics factory
        LiebreContext.setStreamMetrics(Metrics.fileAndConsumer("src/main/resources/queryMetrics", new java.util.HashMap<>()));
        // Notify the Terminator not to end after the first query has completed
        LiebreContext.setSingleQueryExecution(false);
    }

    // Define the objective for the multi-objective optimization
    private final static SequencedMap<String, Comparator<Double>> OBJECTIVES = new TreeMap<>(
            Map.ofEntries(
                    Map.entry("privacy", ((Comparator<Double>) Double::compareTo).reversed()), // Maximize
                    Map.entry("results-similarity", ((Comparator<Double>) Double::compareTo).reversed()), // Maximize
                    Map.entry("metrics-difference", Double::compareTo)  // Minimize
            ));

    private final static Distance<List<AirQualityEvent>> RESULTS_SIMILARITY = new F1Score();
    private final static Distance<MainQuery.PerformanceMetrics> METRICS_DIFFERENCE = new EuclideanDistance();
    private final static Distance<List<AirQualityEvent>> SUPPRESSION_PRIVACY = new SuppressionPrivacy();
    private final static Distance<List<AirQualityEvent>> DUPLICATE_PRIVACY = new DuplicationPrivacy();
    private final static ModificationPrivacy MODIFICATION_PRIVACY = new ModificationPrivacy();

    // Weights for the weighted sum calculation of the final privacy score
    private static final double W_SUPPRESSION = 0.33;
    private static final double W_DUPLICATION = 0.33;
    private static final double W_MODIFICATION = 0.34;

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

                double finalPrivacyScore;

                // If the modified datastream is empty, return 0 as F1 score and maximum difference
                if (modifiedEvents.isEmpty()) {
                    qualities.put("results-similarity", 0.0);
                    qualities.put("metrics-difference", Double.MAX_VALUE);
                    qualities.put("privacy", W_SUPPRESSION * 1.0);
                    return qualities;
                }

                // Calculate the three individual privacy score and combine them into a single value using the defined weights
                double suppressionScore = SUPPRESSION_PRIVACY.apply(this.originalStream, modifiedEvents);
                double duplicationScore = DUPLICATE_PRIVACY.apply(this.originalStream, modifiedEvents);
                double modificationScore = MODIFICATION_PRIVACY.apply(this.originalStream, modifiedEvents, intermediateRepr);
                finalPrivacyScore = (W_SUPPRESSION * suppressionScore) + (W_DUPLICATION * duplicationScore) + (W_MODIFICATION * modificationScore);

                // Execute the main query
                MainQuery.QueryResult modifiedOutcome = MainQuery.process(modifiedEvents, String.valueOf(queryId));

                // Populate the results map with F1 score, Euclidean distance and privacy score
                qualities.put("results-similarity", RESULTS_SIMILARITY.apply(originalResults, modifiedOutcome.events()));
                qualities.put("metrics-difference", METRICS_DIFFERENCE.apply(originalMetrics, modifiedOutcome.metrics()));
                qualities.put("privacy", finalPrivacyScore);
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