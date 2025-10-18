package jgea.representation;

import io.github.ericmedvet.jgea.core.representation.tree.Tree;

import java.util.List;

import static jgea.utils.TreeUtils.findFirstTerminal;

public class TreeToRepresentation {

    // Parse a node <pipeline> recursively, add found operators to a list
    public void parsePipelineNode(Tree<String> pipelineNode, List<QueryRepresentation.OperatorNode> operators) {

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
        QueryRepresentation.OperatorNode operator = parseFilterNode(filterNode);
        if (operator != null) {
            operators.add(operator);
        }

        // Check if the pipeline is finished otherwise recursively calls the method and continue parsing the pipeline
        if (nextPipelineNode != null) {
            parsePipelineNode(nextPipelineNode, operators);
        }
    }

    // Parse a single filter node
    private QueryRepresentation.OperatorNode parseFilterNode(Tree<String> filterNode) {
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
            QueryRepresentation.Operator op = QueryRepresentation.fromString(operatorString);
            QueryRepresentation.Condition condition = new QueryRepresentation.Condition(attribute, op, value);

            return new QueryRepresentation.OperatorNode("FILTER", condition);
        } catch (Exception e) {
            System.err.printf("[Parser] Error parsing filter node: %s\n", e.getMessage());
            return null;
        }
    }
}
