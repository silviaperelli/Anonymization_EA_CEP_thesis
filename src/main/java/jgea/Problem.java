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
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import query.Query;
import utils.Evaluator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

public class Problem implements GrammarBasedProblem<String, QueryRepresentation>,
        TotalOrderQualityBasedProblem<QueryRepresentation, Double> {

    private final StringGrammar<String> grammar;
    private final String inputCsvPath;

    private final List<Sequence> originalCepResults;

    // Constructor that loads the grammar
    public Problem(String grammarPath, String inputCsvPath) throws IOException {
        this.inputCsvPath = inputCsvPath;
        try (FileInputStream fis = new FileInputStream(grammarPath)) {
            this.grammar = StringGrammar.load(fis);
        }
        System.out.println("Loading pre-calculated original CEP results (O)...");
        Path originalResultsPath = Paths.get("src/main/resources/datasets/results/targetDataset.csv");
        this.originalCepResults = Evaluator.parseSequencesFromFile(originalResultsPath);
        System.out.println("Loaded " + this.originalCepResults.size() + " original sequences.");

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
                QueryRepresentation intermediateRepr = new QueryRepresentation(operators);

                System.out.println("[Pipeline Generated] " + intermediateRepr);
                return intermediateRepr;
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
            // Second mapping from PipelineRepresentation to Query Liebre
            String tempOutputFile = "src/main/resources/datasets/evolution/modifiedDataStream/" + intermediateRepr.hashCode() + ".csv";
            System.out.println("[MAPPER] Asking Liebre to write to: " + tempOutputFile);
            RepresentationToLiebreQuery mapper = new RepresentationToLiebreQuery();
            Query query = null;
            try {
                query = mapper.translate(intermediateRepr, this.inputCsvPath, tempOutputFile);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (query == null) {
                return 0.0;
            }

            String outputPathKey = String.valueOf(intermediateRepr.hashCode());
            if (outputPathKey == null) {
                System.err.println("ERROR: Could not find output path for query: " + query.hashCode());
                return 0.0;
            }

            String modifiedDataPath = "src/main/resources/datasets/evolution/modifiedDataStream/" + outputPathKey + ".csv";
            String modifiedCepResultsPath = "/evolution/cepResults/" + outputPathKey + ".csv";

            System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 0. Inizio valutazione fitness.");

            try {
                Path modifiedDataParentDir = Paths.get(modifiedDataPath).getParent();
                if (modifiedDataParentDir != null) {
                    Files.createDirectories(modifiedDataParentDir);
                }

                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 1. Attivazione query per scrivere su: " + modifiedDataPath);
                query.activate();
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 2. Chiamata a query.activate() terminata.");

                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 3. Inizio attesa fissa (Thread.sleep).");
                // Aspetta un po' di tempo che il file venga scritto
                Thread.sleep(10000);
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 4. Fine attesa fissa.");

                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 5. Disattivazione query.");
                query.deActivate();
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 6. Chiamata a query.deActivate() terminata.");

                File file = new File(modifiedDataPath);
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 7. Controllo file. Esiste: " + file.exists() + ", Dimensione: " + file.length());

                if (!file.exists() || file.length() == 0) {
                    System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] File vuoto o inesistente. Valutazione interrotta, fitness = 0.0.");
                    return 0.0;
                }

                long validRows = Files.lines(Path.of(file.toURI()))
                        .filter(line -> !line.startsWith("Date;Time;CO(GT)"))
                        .filter(Objects::nonNull)
                        .count();
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 8. Righe valide nel file: " + validRows);
                if (validRows == 0) {
                    System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] Nessuna riga valida. Valutazione interrotta, fitness = 0.0.");
                    return 0.0;
                }

                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 9. Creazione ambiente Flink per CEP.");
                StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.createLocalEnvironment();

                DataStream<AirQualityEvent> modifiedStream = StreamFactory.createStream(flinkEnv, modifiedDataPath);
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 10. Stream Flink creato.");

                Pattern<AirQualityEvent, ?> cepPattern = PollutionAlertQuery.createHighCoPattern();
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 11. Pattern CEP creato.");

                Path cepOutputParentDir = Paths.get("src/main/resources/datasets" + modifiedCepResultsPath).getParent();
                if (cepOutputParentDir != null) {
                    Files.createDirectories(cepOutputParentDir);
                }

                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 12. Inizio esecuzione CEP (chiamata a findAndProcessAlerts).");
                PollutionAlertQuery.findAndProcessAlerts(modifiedStream, cepPattern, modifiedCepResultsPath);
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 13. Fine esecuzione CEP.");

                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 14. Lettura risultati CEP dal file.");
                List<Sequence> modifiedCepResults = Evaluator.parseSequencesFromFile(Paths.get("src/main/resources/datasets" + modifiedCepResultsPath));
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 15. Numero di sequenze trovate: " + modifiedCepResults.size());

                double f1Score = calculateF1Score(originalCepResults, modifiedCepResults);
                System.out.println("[DEBUG_FITNESS][" + outputPathKey + "] 16. Calcolo F1-Score completato. Fitness = " + f1Score);
                return f1Score;

            } catch (Exception e) {
                System.err.printf(
                        "[DEBUG_FITNESS_ERROR] Errore durante la valutazione della fitness per la query %s%n" +
                                "Type: %s%n" +
                                "Message: %s%n" +
                                "Location: %s%n",
                        outputPathKey,
                        e.getClass().getName(),
                        e.getMessage(),
                        Arrays.stream(e.getStackTrace())
                                .findFirst()
                                .map(StackTraceElement::toString)
                                .orElse("Unknown location")
                );
                return 0.0;
            }
        };
    }

    private double calculateF1Score(List<Sequence> groundTruth, List<Sequence> predictions) {
        int truePositive = 0;
        boolean[] matchedPredictions = new boolean[predictions.size()];

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
