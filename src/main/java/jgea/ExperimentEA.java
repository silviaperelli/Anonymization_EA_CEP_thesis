package jgea;

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
import jgea.representation.QueryRepresentation;
import jgea.utils.CSVAnalyzer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


// Launch a small genetic algorithm to test the evolutionary process with a small population
public class ExperimentEA {

    public static void main(String[] args) throws IOException {
        final String csvPath = "datasets/airQuality.csv";
        final String grammarPath = "generated-grammar.bnf";

        // Grammar generation based on numerical bounds and loading
        List<String> attributes = CSVAnalyzer.extractAttributes(csvPath);
        Map<String, CSVAnalyzer.AttributeStats> statsMap = CSVAnalyzer.analyze(csvPath);
        GrammarGenerator.generateGrammar(attributes, statsMap, grammarPath);
        Problem problem = new Problem(grammarPath, csvPath);
        StringGrammar<String> grammar = problem.grammar();

        // Define generic operator
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

                double bestFitness = problem.qualityFunction().apply(bestSolution);

                System.out.println("\n--- Best Solution Found ---");
                System.out.printf("Pipeline: %s%n", bestSolution);
                System.out.printf("Fitness (F1-Score): %.4f%n", bestFitness);
                System.out.println("---------------------------");

            } else {
                System.out.println("\nNo solution found.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
            System.exit(0);
        }
    }
}
