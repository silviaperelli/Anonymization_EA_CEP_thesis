package jgea;

import io.github.ericmedvet.jgea.core.operator.GeneticOperator;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrammarBasedSubtreeMutation;
import io.github.ericmedvet.jgea.core.representation.grammar.string.cfggp.GrammarRampedHalfAndHalf;
import io.github.ericmedvet.jgea.core.representation.tree.*;
import io.github.ericmedvet.jgea.core.selector.Last;
import io.github.ericmedvet.jgea.core.selector.Tournament;
import io.github.ericmedvet.jgea.core.solver.StandardEvolver;
import io.github.ericmedvet.jgea.core.solver.StopConditions;
import jgea.utils.CSVHeaderReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


// Launch a small genetic algorithm to test the evolutionary process with a small population
public class ExampleEA {
    public static void main(String[] args) throws IOException {
        // Grammar generation and loading
        final String CsvPath = "datasets/airQuality.csv";
        final String GrammarPath = "generated-grammar.bnf";
        List<String> CsvAttributes = CSVHeaderReader.extractAttributes(CsvPath);
        GrammarGenerator.generateGrammar(CsvAttributes, GrammarPath);
        Problem problem = new Problem(GrammarPath);
        StringGrammar<String> grammar = problem.grammar();

        // Define generic operator
        Map<GeneticOperator<Tree<String>>, Double> operators = Map.of(
                // Crossover
                new SameRootSubtreeCrossover<>(12), 0.8,
                // Mutation based on the grammar
                new GrammarBasedSubtreeMutation<>(12, grammar), 0.2
        );


        // Solver configuration
        GrammarRampedHalfAndHalf<String> factory = new GrammarRampedHalfAndHalf<>(3, 8, grammar);
        StandardEvolver<Tree<String>, PipelineRepresentation, Double> solver = new StandardEvolver<>(
                problem.solutionMapper(),
                factory,
                100,
                StopConditions.nOfFitnessEvaluations(5000),
                operators,
                new Tournament(5),
                new Last(),
                100,
                true,
                100,
                false,
                List.of()
        );

        // Execution
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            System.out.println("Start of execution");
            Collection<PipelineRepresentation> solutions = solver.solve(
                    problem, new Random(1), executor
            );

            System.out.println("Evolution finished");
            if (!solutions.isEmpty()) {
                PipelineRepresentation bestSolution = solutions.iterator().next();
                System.out.println("\n--- Best solution found ---");
                System.out.println("Phenotype: " + bestSolution);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
        }

    }
}
