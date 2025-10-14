package jgea;

import io.github.ericmedvet.jgea.core.problem.TotalOrderQualityBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.GrammarBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;
import jgea.representation.PipelineRepresentation;
import jgea.representation.RepresentationToLiebreQuery;
import query.Query;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import static jgea.utils.TreeUtils.*;

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
                parsePipelineNode(tree, operators);
                PipelineRepresentation intermediateRepr = new PipelineRepresentation(operators);

                System.out.println("[Pipeline Generated] " + intermediateRepr);

                // Second mapping from PipelineRepresentation to Query Liebre
                RepresentationToLiebreQuery mapper = new RepresentationToLiebreQuery();
                String tempOutputFile = "src/main/resources/datasets/testBestQuery.csv";
                return mapper.translate(intermediateRepr, this.inputCsvPath, tempOutputFile);
            }catch(Exception e){
                System.err.println(("Error during mapping process"));
                return null;
            }
        };
    }

    // Parse a node <pipeline> recursively, add found operators to a list
    private void parsePipelineNode(Tree<String> pipelineNode, List<PipelineRepresentation.OperatorNode> operators) {

        // Look for the child node <filter> and <pipeline>
        Tree<String> filterNode = null;
        Tree<String> nextPipelineNode = null;

        for (Tree<String> child : pipelineNode) {
            if ("<filter>".equals(child.content())) {
                filterNode = child;
            } else if ("<pipeline>".equals(child.content())) {
                nextPipelineNode = child;
            }
        }

        // The tree is invalid if there isn't a node <filter>
        if (filterNode == null) {
            return;
        }

        // Parse filter node
        PipelineRepresentation.OperatorNode operator = parseFilterNode(filterNode);
        if (operator != null) {
            operators.add(operator);
        }

        // Check if the pipeline is finished otherwise recursively calls the method and continue parsing the pipeline
        if (nextPipelineNode != null) {
            parsePipelineNode(nextPipelineNode, operators);
        }
    }


    // Parse a single filter node
    private PipelineRepresentation.OperatorNode parseFilterNode(Tree<String> filterNode) {
        String attribute = null;
        String operatorString = null;
        Tree<String> valueNode = null;

        // Look for the children in the node
        for (Tree<String> child : filterNode) {
            switch (child.content()) {
                case "<attribute>" -> attribute = findFirstTerminal(child);
                case "<operator>" -> operatorString = findFirstTerminal(child);
                case "<value>" -> valueNode = child;
            }
        }

        if (attribute == null || operatorString == null || valueNode == null)
            return null;

        // Mapping from tree to Pipeline Representation
        try {
            // Collect all the leaves (digit and .) under the node <value> and join them to reconstruct the number
            List<String> leaves = valueNode.visitLeaves();
            String valueString = String.join("", leaves);
            double value = Double.parseDouble(valueString);
            attribute = attribute.replace("'", "");
            PipelineRepresentation.Operator op = PipelineRepresentation.fromString(operatorString);
            PipelineRepresentation.Condition condition = new PipelineRepresentation.Condition(attribute, op, value);

            return new PipelineRepresentation.OperatorNode("FILTER", condition);
        } catch (Exception e) {
            System.err.printf("[Parser] Error parsing filter node: %s\n", e.getMessage());
            return null;
        }
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
