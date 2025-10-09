package jgea;

import io.github.ericmedvet.jgea.core.problem.TotalOrderQualityBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.GrammarBasedProblem;
import io.github.ericmedvet.jgea.core.representation.grammar.string.StringGrammar;
import io.github.ericmedvet.jgea.core.representation.tree.Tree;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;

import static jgea.utils.TreeUtils.*;

public class Problem implements GrammarBasedProblem<String, PipelineRepresentation>,
        TotalOrderQualityBasedProblem<PipelineRepresentation, Double> {

    private final StringGrammar<String> grammar;

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
                    System.err.println("[Mapper] Error while parsing filter: " + e.getMessage());
                }
            }

            return new PipelineRepresentation(operators);
        };
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
            StringBuilder sb = new StringBuilder();
            for (String leaf : leaves) {
                if (leaf.matches("[0-9]") || leaf.equals(".")) {
                    sb.append(leaf);
                }
            }
            double value = Double.parseDouble(sb.toString());
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
    public Function<PipelineRepresentation, Double> qualityFunction() {
        // The fitness function only assigns a random number to each pipeline created
        return pipeline -> new Random().nextDouble();
    }
}
