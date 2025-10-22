package jgea.oldExperiment;

import io.github.ericmedvet.jgea.core.operator.GeneticOperator;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrammarBasedSubtreeMutation;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrammarRampedHalfAndHalf;
import io.github.ericmedvet.jgea.core.representation.tree.SameRootSubtreeCrossover;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import io.github.ericmedvet.jgea.core.selector.Last;
import io.github.ericmedvet.jgea.core.selector.Tournament;
import io.github.ericmedvet.jgea.core.solver.StandardEvolver;
import io.github.ericmedvet.jgea.core.solver.StopConditions;
import jgea.mappers.QueryRepresentation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


// Launch a small genetic algorithm to test the evolutionary process with a small population
public class ExperimentEA {

    public static void main(String[] args) throws IOException, InterruptedException {

        final String csvPath = "datasets/airQuality.csv";
        String grammarPath = "generated-grammar.bnf";

        Problem problem = new Problem(grammarPath, csvPath);
        StringGrammar<String> grammar = problem.grammar();

        // Define genetic operators
        Map<GeneticOperator<Tree<String>>, Double> operators = Map.of(
                // Crossover
                new SameRootSubtreeCrossover<>(12), 0.8,
                // Mutation based on the grammar
                new GrammarBasedSubtreeMutation<>(12, grammar), 0.2
        );

        int populationSize = 20;
        int nOfEvaluations = 100;

        System.out.println("--- JGEA Evolution Parameters ---");
        System.out.printf("Population size: %d%n", populationSize);
        System.out.printf("Max evaluations: %d%n", nOfEvaluations);
        System.out.println("---------------------------------");

        // Solver configuration
        GrammarRampedHalfAndHalf<String> factory = new GrammarRampedHalfAndHalf<>(3, 8, grammar);
        StandardEvolver<Tree<String>, QueryRepresentation, Double> solver = new StandardEvolver<>(
                problem.solutionMapper(),
                factory,
                populationSize,
                StopConditions.nOfFitnessEvaluations(nOfEvaluations),
                operators,
                new Tournament(5),
                new Last(),
                populationSize,
                true,
                populationSize,
                false,
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
