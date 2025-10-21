package jgea.newProblem;

import io.github.ericmedvet.jgea.core.operator.GeneticOperator;
import io.github.ericmedvet.jgea.core.problem.MultiObjectiveProblem;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrammarBasedSubtreeMutation;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrammarRampedHalfAndHalf;
import io.github.ericmedvet.jgea.core.representation.tree.SameRootSubtreeCrossover;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import io.github.ericmedvet.jgea.core.selector.Last;
import io.github.ericmedvet.jgea.core.selector.Tournament;
import io.github.ericmedvet.jgea.core.solver.NsgaII;
import io.github.ericmedvet.jgea.core.solver.StandardEvolver;
import io.github.ericmedvet.jgea.core.solver.StopConditions;
import jgea.representation.QueryRepresentation;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NewExperimentEA {

    public static void main(String[] args) throws Exception { // Aggiungi throws Exception

        String grammarPath = "generated-grammar.bnf";
        String inputCsvPath = "datasets/airQuality.csv";
        int populationSize = 5;
        int nOfEvaluations = 10;

        // --- 2. CREA IL PROBLEMA ---
        // (Nota: il costruttore del problema ora non ha più bisogno della grammatica)
        SimpleMOProblem problem = new StreamAnonymizationProblem(inputCsvPath);

        // --- 3. CREA IL MAPPER ---
        // Crea un'istanza del tuo nuovo mapper.
        TreeToQueryRepresentationMapper mapper = new TreeToQueryRepresentationMapper();

        // --- 4. CARICA LA GRAMMATICA (necessaria per la factory e gli operatori) ---
        StringGrammar<String> grammar;
        try (FileInputStream fis = new FileInputStream(grammarPath)) {
            grammar = StringGrammar.load(fis);
        }

        // --- 5. CONFIGURA IL SOLVER (StandardEvolver) ---
        // Factory per creare la popolazione iniziale
        GrammarRampedHalfAndHalf<String> factory = new GrammarRampedHalfAndHalf<>(3, 8, grammar);

        // Operatori genetici (crossover, mutazione)
        Map<GeneticOperator<Tree<String>>, Double> operators = Map.of(
                new SameRootSubtreeCrossover<>(12), 0.8,
                new GrammarBasedSubtreeMutation<>(12, grammar), 0.2
        );

        NsgaII<Tree<String>, QueryRepresentation, Map<String, Double>> solver = new NsgaII<>(
                // Argomento 1: il mapper
                mapper,
                // Argomento 2: la factory
                factory,
                // Argomento 3: la dimensione della popolazione
                populationSize,
                // Argomento 4: la condizione di stop
                StopConditions.nOfFitnessEvaluations(nOfEvaluations),
                // Argomento 5: la mappa degli operatori genetici
                operators,
                // Argomento 6: tentativi massimi di unicità
                100,
                // Argomento 7: remap
                true,
                // Argomento 8: comparatori addizionali (per ora, una lista vuota)
                List.of()
        );


        // Execution
        int nThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        System.out.println("Starting evolution with " + nThreads + " threads.");
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        //ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            System.out.println("Start of execution");
            Collection<QueryRepresentation> solutions = solver.solve(
                    problem, new Random(1), executor
            );
            System.out.println("\nEvolution finished!");
            if (!solutions.isEmpty()) {
                QueryRepresentation bestSolution = solutions.iterator().next();

                System.out.println("\n--- Best Solution Found ---");
                System.out.printf("Pipeline: %s%n", bestSolution);
                System.out.println("---------------------------");
            } else {
                System.out.println("\nNo solution found.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Start the executor shutdown
            executor.shutdown();

            // Wait for the evaluation tasks to finish
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Evaluation tasks did not terminate within the expected time. Forcing shutdown");
                executor.shutdownNow();
            } else {
                System.out.println("All evaluation tasks terminated successfully");
            }

            // JVM shutdown for any background threads
            System.out.println("The process has terminated");
            System.exit(0);
        }
    }
}