package jgea.problem;

import event.AirQualityEvent;
import event.StreamFactory;
import io.github.ericmedvet.jgea.core.distance.Distance;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import jgea.mappers.QueryRepresentation;
import jgea.metrics.privacy.*;
import jgea.metrics.results.F1Score;
import jgea.problem.utils.PrivacyMetricChoice;
import jgea.query.LiebreAnonymizationQuery;
import jgea.query.MainQueryKeys;
import query.LiebreContext;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

// Define the multi-objective optimization problem
public class AnonymizationProblem_2Objectives implements SimpleMOProblem<QueryRepresentation, Double> {

    // Define a static counter for unique query ID
    private static final AtomicLong queryCounter = new AtomicLong(0);

    static {
        //Since metrics will be added and removed during the query execution, we need to
        //ensure that the LiebreContext is initialized with the right metrics factory
        //LiebreContext.setStreamMetrics(Metrics.fileAndConsumer("src/main/resources/queryMetrics", new java.util.HashMap<>()));
        // Notify the Terminator not to end after the first query has completed
        LiebreContext.setSingleQueryExecution(false);
    }

    // Define the objective to maximize for the multi-objective optimization
    private final static SequencedMap<String, Comparator<Double>> OBJECTIVES = new TreeMap<>(
            Map.ofEntries(
                    Map.entry("privacy", ((Comparator<Double>) Double::compareTo).reversed()),
                    Map.entry("results-similarity", ((Comparator<Double>) Double::compareTo).reversed())
            ));

    private final static Distance<List<AirQualityEvent>> RESULTS_SIMILARITY = new F1Score();

    private final Distance<List<AirQualityEvent>> K_ANONYMITY_PRIVACY;
    private final Distance<List<AirQualityEvent>> K_ANONYMITY_PRIVACY_CARDINALITY;
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

    private final PrivacyMetricChoice privacyMetricChoice;

    public AnonymizationProblem_2Objectives(String inputCsvPath, PrivacyMetricChoice privacyMetric) throws Exception {
        this.inputCsvPath = inputCsvPath;

        // Load the original stream of events from the CSV file
        this.originalStream = StreamFactory.createListFromFile(inputCsvPath);
        this.privacyMetricChoice = privacyMetric;

        K_ANONYMITY_PRIVACY = new KAnonymityPrivacy(this.originalStream, 50);
        K_ANONYMITY_PRIVACY_CARDINALITY = new KAnonymityPrivacyCardinality(this.originalStream, 50);

        // Execute the main query
        MainQueryKeys.QueryResult baselineOutcome = MainQueryKeys.process(this.originalStream, "original");

        this.originalResults = baselineOutcome.events();

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

                // Case with empty modified datastream
                if (modifiedEvents.isEmpty()) {
                    // Based on the user choice, calculate the correct privacy metric
                    double privacyScoreEmpty;
                    switch (privacyMetricChoice) {
                        case SUPPRESSION_ONLY -> privacyScoreEmpty = 1.0;
                        case WEIGHTED_AVERAGE -> privacyScoreEmpty = W_SUPPRESSION;
                        case K_ANONYMITY -> privacyScoreEmpty = 0.0;
                        case K_ANONYMITY_CARDINALITY -> privacyScoreEmpty = K_ANONYMITY_PRIVACY_CARDINALITY.apply(this.originalStream, modifiedEvents);
                        default -> privacyScoreEmpty = K_ANONYMITY_PRIVACY_CARDINALITY.apply(this.originalStream, modifiedEvents);
                    }
                    qualities.put("privacy", privacyScoreEmpty);
                    qualities.put("results-similarity", 0.0);
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
                    case K_ANONYMITY_CARDINALITY:
                    default: // Default alla tua metrica più avanzata
                        finalPrivacyScore = K_ANONYMITY_PRIVACY_CARDINALITY.apply(this.originalStream, modifiedEvents);
                        break;
                }

                // Execute the main query
                MainQueryKeys.QueryResult modifiedOutcome = MainQueryKeys.process(modifiedEvents, String.valueOf(queryId));

                // Populate the results map with F1 score, Euclidean distance and privacy score
                qualities.put("results-similarity", RESULTS_SIMILARITY.apply(originalResults, modifiedOutcome.events()));
                qualities.put("privacy", finalPrivacyScore);
                return qualities;

            } catch (Exception e) {
                System.err.printf("Error during fitness evaluation: %s", e.getMessage());
                e.printStackTrace();
                qualities.put("results-similarity", 0.0);
                qualities.put("privacy", 0.0);
                return qualities;
            }
        };
    }
}