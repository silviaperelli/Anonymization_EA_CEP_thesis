package jgea.problem;

import event.DataLoader;
import event.GenericEvent;
import io.github.ericmedvet.jgea.core.distance.Distance;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import jgea.mappers.QueryRepresentation;
import jgea.metrics.performance.PerformanceSimilarity;
import jgea.metrics.privacy.*;
import jgea.metrics.results.F1Score;
import jgea.problem.utils.PrivacyMetricChoice;
import jgea.query.LiebreAnonymizationQuery;
import jgea.query.MainQueryKeys;
import query.LiebreContext;
import jgea.metrics.performance.utils.StreamStatsWindow;

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
        //LiebreContext.setStreamMetrics(Metrics.fileAndConsumer("src/main/resources/queryMetrics", new HashMap<>()));
        // Notify the Terminator not to end after the first query has completed
        LiebreContext.setSingleQueryExecution(false);
    }

    // Define the objective to maximize for the multi-objective optimization
    private final static SequencedMap<String, Comparator<Double>> OBJECTIVES = new TreeMap<>(
            Map.ofEntries(
                    Map.entry("privacy", ((Comparator<Double>) Double::compareTo).reversed()),
                    Map.entry("results-similarity", ((Comparator<Double>) Double::compareTo).reversed()),
                    Map.entry("performance-similarity", ((Comparator<Double>) Double::compareTo).reversed())
            ));

    private final static Distance<List<GenericEvent>> RESULTS_SIMILARITY = new F1Score();
    private final Distance<StreamStatsWindow> PERFORMANCE_SIMILARITY;

    private final Distance<List<GenericEvent>> K_ANONYMITY_PRIVACY;
    private final Distance<List<GenericEvent>> K_ANONYMITY_PRIVACY_CARDINALITY;
    private final KAnonymityPrivacyCardinalityStats K_ANONYMITY_STATS;
    private final static Distance<List<GenericEvent>> SUPPRESSION_PRIVACY = new SuppressionPrivacy();
    private final static Distance<List<GenericEvent>> DUPLICATE_PRIVACY = new DuplicationPrivacy();
    private final ModificationPrivacy MODIFICATION_PRIVACY;

    // Weights for the weighted sum calculation of the final privacy score
    private static final double W_SUPPRESSION = 0.33;
    private static final double W_DUPLICATION = 0.33;
    private static final double W_MODIFICATION = 0.34;

    private final String inputCsvPath;
    private final String keyColumn;
    private final List<GenericEvent> originalStream;
    private final List<GenericEvent> originalResults; // Ground truth results, calculated once in the constructor
    private final StreamStatsWindow originalStats;

    private final PrivacyMetricChoice privacyMetricChoice;
    private final List<String> attributes;

    public StreamAnonymizationProblem(String inputCsvPath, String keyColumn, PrivacyMetricChoice privacyMetric, boolean isFilterOnly) throws Exception {
        this.inputCsvPath = inputCsvPath;
        this.keyColumn = keyColumn;

        // Load the original stream of events from the CSV file
        DataLoader loader = new DataLoader(inputCsvPath, keyColumn);
        DataLoader.LoadResult result = loader.load();

        this.originalStream = result.events();
        this.attributes = result.numericAttributes();
        this.privacyMetricChoice = privacyMetric;

        K_ANONYMITY_PRIVACY = new KAnonymityPrivacy(this.originalStream, 50, this.attributes);
        K_ANONYMITY_PRIVACY_CARDINALITY = new KAnonymityPrivacyCardinality(this.originalStream, 50, this.attributes);
        K_ANONYMITY_STATS = new KAnonymityPrivacyCardinalityStats(this.originalStream, 50, this.attributes);
        MODIFICATION_PRIVACY = new ModificationPrivacy(this.attributes);

        // Execute the main query
        MainQueryKeys.QueryResult baselineOutcome = MainQueryKeys.process(this.originalStream, "original");

        this.originalResults = baselineOutcome.events();
        this.originalStats = baselineOutcome.statsWindow();

        PERFORMANCE_SIMILARITY = new PerformanceSimilarity(this.originalStats, isFilterOnly);

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
                List<GenericEvent> modifiedEvents = liebreExecutor.processAnonymizationQuery(intermediateRepr, this.inputCsvPath, this.keyColumn);

                // Case with empty modified datastream
                if (modifiedEvents.isEmpty()) {
                    double privacyScoreEmpty;
                    // Based on the user choice, calculate the correct privacy metric
                    switch (privacyMetricChoice) {
                        case SUPPRESSION_ONLY -> privacyScoreEmpty = 1.0;
                        case WEIGHTED_AVERAGE -> privacyScoreEmpty = W_SUPPRESSION;
                        case K_ANONYMITY -> privacyScoreEmpty = 0.0;
                        case K_ANONYMITY_CARDINALITY_MAX -> privacyScoreEmpty = K_ANONYMITY_STATS.applyWithMax(this.originalStream, modifiedEvents);
                        case K_ANONYMITY_CARDINALITY_Q99 -> privacyScoreEmpty = K_ANONYMITY_STATS.applyWithQuantile99(this.originalStream, modifiedEvents);
                        case K_ANONYMITY_CARDINALITY -> privacyScoreEmpty = K_ANONYMITY_PRIVACY_CARDINALITY.apply(this.originalStream, modifiedEvents);
                        default -> privacyScoreEmpty = K_ANONYMITY_PRIVACY_CARDINALITY.apply(this.originalStream, modifiedEvents);
                    }
                    qualities.put("privacy", privacyScoreEmpty);
                    qualities.put("results-similarity", 0.0);
                    StreamStatsWindow emptyStats = new StreamStatsWindow(
                            originalStats.streamNames(),
                            originalStats.minTimestamp(),
                            originalStats.maxTimestamp(),
                            originalStats.getResolutionMillis());
                    qualities.put("performance-similarity", PERFORMANCE_SIMILARITY.apply(this.originalStats, emptyStats));
                    return qualities;
                }

                // Based on the user choice, calculate the correct privacy metric
                double finalPrivacyScore;
                switch (privacyMetricChoice) {
                    case K_ANONYMITY:
                        finalPrivacyScore = K_ANONYMITY_PRIVACY.apply(this.originalStream, modifiedEvents);
                        break;
                    case WEIGHTED_AVERAGE:
                        double suppressionScore = SUPPRESSION_PRIVACY.apply(this.originalStream, modifiedEvents);
                        double duplicationScore = DUPLICATE_PRIVACY.apply(this.originalStream, modifiedEvents);
                        double modificationScore = MODIFICATION_PRIVACY.apply(this.originalStream, modifiedEvents, intermediateRepr);
                        finalPrivacyScore = (W_SUPPRESSION * suppressionScore) + (W_DUPLICATION * duplicationScore) + (W_MODIFICATION * modificationScore);
                        break;
                    case SUPPRESSION_ONLY:
                        finalPrivacyScore = SUPPRESSION_PRIVACY.apply(this.originalStream, modifiedEvents);
                        break;
                    case K_ANONYMITY_CARDINALITY_MAX:
                        finalPrivacyScore = K_ANONYMITY_STATS.applyWithMax(this.originalStream, modifiedEvents);
                        break;
                    case K_ANONYMITY_CARDINALITY_Q99:
                        finalPrivacyScore = K_ANONYMITY_STATS.applyWithQuantile99(this.originalStream, modifiedEvents);
                        break;
                    case K_ANONYMITY_CARDINALITY:
                    default:
                        finalPrivacyScore = K_ANONYMITY_PRIVACY_CARDINALITY.apply(this.originalStream, modifiedEvents);
                        break;
                }

                // Execute the main query
                MainQueryKeys.QueryResult modifiedOutcome = MainQueryKeys.process(modifiedEvents, String.valueOf(queryId));

                StreamStatsWindow modifiedStats = modifiedOutcome.statsWindow();
                qualities.put("performance-similarity", PERFORMANCE_SIMILARITY.apply(originalStats, modifiedStats));

                // Populate the results map with F1 score, Euclidean distance and privacy score
                qualities.put("results-similarity", RESULTS_SIMILARITY.apply(originalResults, modifiedOutcome.events()));
                qualities.put("privacy", finalPrivacyScore);
                return qualities;

            } catch (Exception e) {
                System.err.printf("Error during fitness evaluation: %s", e.getMessage());
                e.printStackTrace();
                qualities.put("results-similarity", 0.0);
                qualities.put("performance-similarity", 0.0);
                qualities.put("privacy", 0.0);
                return qualities;
            }
        };
    }
}

