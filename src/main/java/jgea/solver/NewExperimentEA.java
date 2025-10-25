package jgea.solver;

import io.github.ericmedvet.jgea.core.operator.GeneticOperator;
import io.github.ericmedvet.jgea.core.problem.SimpleMOProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrammarBasedSubtreeMutation;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrammarRampedHalfAndHalf;
import io.github.ericmedvet.jgea.core.representation.tree.SameRootSubtreeCrossover;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import io.github.ericmedvet.jgea.core.solver.NsgaII;
import io.github.ericmedvet.jgea.core.solver.StopConditions;
import jgea.problem.StreamAnonymizationProblem;
import query.LiebreContext;
import jgea.mappers.Mapper;
import jgea.mappers.QueryRepresentation;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import common.metrics.Metrics;


// Main class for the evolutionary experiment, it initializes all components and execute the solver
// Will be replaced by an experiment txt file
public class NewExperimentEA {

    public static void main(String[] args) throws Exception {

        /*
         * Since metrics will be added and rmoved during the query execution, we need to
         * ensure that the LiebreContext is initialized with the right metrics factory
         */
        LiebreContext.setStreamMetrics(Metrics.fileAndConsumer("src/main/resources", new java.util.HashMap<>()));
        
        String grammarPath = "generated-grammar.bnf";
        String inputCsvPath = "datasets/airQuality.csv";
        int populationSize = 5;
        int nOfEvaluations = 10;

        System.out.println("--- JGEA Evolution Parameters ---");
        System.out.printf("Population size: %d%n", populationSize);
        System.out.printf("Max evaluations: %d%n", nOfEvaluations);
        System.out.println("---------------------------------");

        // Instantiate the multi-objective problem
        SimpleMOProblem problem = new StreamAnonymizationProblem(inputCsvPath);

        // Instantiate the mapper
        Mapper mapper = new Mapper();

        // Load the context-free grammar from a file
        StringGrammar<String> grammar;
        try (FileInputStream fis = new FileInputStream(grammarPath)) {
            grammar = StringGrammar.load(fis);
        }

        // Define the genetic operators (crossover and mutation)
        Map<GeneticOperator<Tree<String>>, Double> operators = Map.of(
                new SameRootSubtreeCrossover<>(12), 0.8,
                new GrammarBasedSubtreeMutation<>(12, grammar), 0.2
        );

        // Solver configuration
        GrammarRampedHalfAndHalf<String> factory = new GrammarRampedHalfAndHalf<>(3, 8, grammar);
        NsgaII<Tree<String>, QueryRepresentation, Map<String, Double>> solver = new NsgaII<>(
                mapper.mapperFor(null),
                factory,
                populationSize,
                StopConditions.nOfFitnessEvaluations(nOfEvaluations),
                operators,
                100,
                true,
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