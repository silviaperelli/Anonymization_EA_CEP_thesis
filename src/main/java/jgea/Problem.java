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

import static jgea.utils.TreeUtils.*;

public class Problem implements GrammarBasedProblem<String, PipelineRepresentation>, TotalOrderQualityBasedProblem<PipelineRepresentation, Double> {

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
                    System.err.println("[Mapper] Error while parsing the filter: " + e.getMessage());
                }
            }
            return new PipelineRepresentation(operators);
        };
    }

    // Parse a single filter node
    private PipelineRepresentation.OperatorNode parseFilterNode(Tree<String> filtroNode) {
        String attribute = null;
        String opString = null;
        Tree<String> valueNode = null;

        // Look for the children in the node
        for (Tree<String> child : filtroNode) {
            switch (child.content()) {
                case "<attribute>" -> attribute = findFirstTerminal(child);
                case "<operator>" -> opString = findFirstTerminal(child);
                case "<value>" -> valueNode = child;
            }
        }

        if (attribute == null || opString == null || valueNode == null) {
            return null;
        }

        // Mapping from tree to Pipeline Representation
        try {
            // Collect all the leaves (digit and .) under the node <value>
            List<String> leaves = valueNode.visitLeaves();

            // Join the leaves to reconstruct the number
            String valueString = String.join("", leaves);
            double value = Double.parseDouble(valueString);

            attribute = attribute.replace("'", "");
            opString = opString.replace("\"", "");
            PipelineRepresentation.Operator operatore = PipelineRepresentation.fromString(opString);
            PipelineRepresentation.Condition condition = new PipelineRepresentation.Condition(attribute, operatore, value);

            return new PipelineRepresentation.OperatorNode("FILTER", condition);

        } catch (IllegalArgumentException e) {
            System.err.printf("[Parser] Filter discarded for format error: %s\n", e.getMessage());
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
