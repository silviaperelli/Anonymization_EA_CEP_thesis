package jgea;

import io.github.ericmedvet.jgea.core.problem.TotalOrderQualityBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.GrammarBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Predicate;

public class Problem implements GrammarBasedProblem<String, PipelineRepresentation>, TotalOrderQualityBasedProblem<PipelineRepresentation, Double> {

    private final StringGrammar<String> grammar;
    private static final Random RANDOM = new Random();

    // Constructor that loads the grammar
    public Problem(String grammarPath) throws IOException {
        try (FileInputStream fis = new FileInputStream(grammarPath)) {
            this.grammar = StringGrammar.load(fis);
        }
    }

    @Override
    public StringGrammar<String> grammar() {
        return grammar;
    }

    @Override
    public Function<Tree<String>, PipelineRepresentation> solutionMapper() {
        return (Tree<String> tree) -> {
            List<PipelineRepresentation.OperatorNode> operators = new ArrayList<>();

            // Find all filter nodes in the tree
            List<Tree<String>> filterNodes = findAllNodes(tree, node -> node.content().equals("<filter>"));

            // Parse each filter node
            for (Tree<String> filterNode : filterNodes) {
                try {
                    PipelineRepresentation.OperatorNode operatorNode = parseFilterNode(filterNode);
                    if (operatorNode != null) {
                        operators.add(operatorNode);
                    }
                } catch (Exception e) {
                    System.err.println("[Mapper] Error while parsing a filter: " + e.getMessage());
                }
            }
            return new PipelineRepresentation(operators);
        };
    }

    // Parse a single filter node
    private PipelineRepresentation.OperatorNode parseFilterNode(Tree<String> filtroNode) {
        String attribute = null;
        String opString = null;
        String valueString = null;

        // Look for the children in the node
        for (Tree<String> child : filtroNode) {
            switch (child.content()) {
                case "<attribute>" -> attribute = findFirstTerminal(child);
                case "<operator>" -> opString = findFirstTerminal(child);
                case "<value>" -> valueString = findFirstTerminal(child);
            }
        }

        if (attribute == null || opString == null || valueString == null) {
            return null;
        }

        // Mapping from tree to Pipeline Representation
        try {
            attribute = attribute.replace("'", "");

            // Generate a random value for the threshold
            double value;
            if (valueString.equals("<double>")) {
                value = RANDOM.nextDouble() * 1000;
            } else {
                value = Double.parseDouble(valueString);
            }

            PipelineRepresentation.Operator operator = PipelineRepresentation.fromString(opString.replace("\"", ""));
            PipelineRepresentation.Condition condition = new PipelineRepresentation.Condition(attribute, operator, value);

            return new PipelineRepresentation.OperatorNode("FILTER", condition);

        } catch (IllegalArgumentException e) {
            System.err.printf("[Parser] Filter discarded for format error: %s", e.getMessage());
            return null;
        }
    }

    // Find first terminal node in a subtree recursively
    private String findFirstTerminal(Tree<String> tree) {
        if (tree.isLeaf()) {
            return tree.content();
        }
        for (Tree<String> child : tree) {
            String terminal = findFirstTerminal(child);
            if (terminal != null) {
                return terminal;
            }
        }
        return null;
    }

    // Find all nodes that match a condition
    private List<Tree<String>> findAllNodes(Tree<String> tree, Predicate<Tree<String>> condition) {
        List<Tree<String>> foundNodes = new ArrayList<>();
        if (condition.test(tree)) {
            foundNodes.add(tree);
        }
        for (Tree<String> child : tree) {
            foundNodes.addAll(findAllNodes(child, condition));
        }
        return foundNodes;
    }

    @Override
    public Comparator<Double> totalOrderComparator() {
        return Comparator.reverseOrder(); // Maximize fitness
    }

    @Override
    public Function<PipelineRepresentation, Double> qualityFunction() {
        // The fitness function only assigns a random number to each pipeline created
        return pipeline -> RANDOM.nextDouble();
    }


}
