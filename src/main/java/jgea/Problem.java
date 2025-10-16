package jgea;

import io.github.ericmedvet.jgea.core.problem.TotalOrderQualityBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.GrammarBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import jgea.representation.PipelineRepresentation;
import jgea.representation.RepresentationToLiebreQuery;
import jgea.representation.TreeToRepresentation;
import query.Query;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;

public class Problem implements GrammarBasedProblem<String, Query>,
        TotalOrderQualityBasedProblem<Query, Double> {

    private final StringGrammar<String> grammar;
    private final String inputCsvPath;

    // Constructor that loads the grammar
    public Problem(String grammarPath, String inputCsvPath) throws IOException {
        this.inputCsvPath = inputCsvPath;
        try (FileInputStream fis = new FileInputStream(grammarPath)) {
            this.grammar = StringGrammar.load(fis);
        }
    }

    @Override
    public StringGrammar<String> grammar() {
        return grammar;
    }

    @Override
    public Function<Tree<String>, Query> solutionMapper() {
        return (Tree<String> tree) -> {
            try{
                // First mapping from tree to PipelineRepresentation
                List<PipelineRepresentation.OperatorNode> operators = new ArrayList<>();
                // Start recursive parsing from the root of the tree
                TreeToRepresentation firstMapper = new TreeToRepresentation();
                firstMapper.parsePipelineNode(tree, operators);
                PipelineRepresentation intermediateRepr = new PipelineRepresentation(operators);

                System.out.println("[Pipeline Generated] " + intermediateRepr);

                // Second mapping from PipelineRepresentation to Query Liebre
                RepresentationToLiebreQuery secondMapper = new RepresentationToLiebreQuery();
                String tempOutputFile = "src/main/resources/datasets/testBestQuery.csv";
                return secondMapper.translate(intermediateRepr, this.inputCsvPath, tempOutputFile);
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
    public Function<Query, Double> qualityFunction() {
        // The fitness function only assigns a random number to each pipeline created
        return query -> {
            if (query == null){
                return 0.0;
            }
            return new Random().nextDouble();
        };
    }
}
