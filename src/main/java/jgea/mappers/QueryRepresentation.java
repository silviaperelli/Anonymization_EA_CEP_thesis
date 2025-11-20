package jgea.mappers;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

// Query Representation (phenotype)
public record QueryRepresentation(
        // Contains a sequence of operators
        List<OperatorNode> operators
) implements Serializable {

    public enum Operator{
        FILTER,
        MAP_DUPLICATE,
        MAP_NOISE
    }
    public enum Condition implements Serializable {
        LESS_THAN, GREATER_THAN, LESS_OR_EQUAL, GREATER_OR_EQUAL
    }

    public static Condition fromString(String text) {
        return switch (text) {
            case "lt" -> Condition.LESS_THAN;
            case "gt" -> Condition.GREATER_THAN;
            case "le" -> Condition.LESS_OR_EQUAL;
            case "ge" -> Condition.GREATER_OR_EQUAL;
            default -> throw new IllegalArgumentException("Condition not valid: " + text);
        };
    }

    // Textual representation printed at the end of evolution
    @Override
    public String toString() {
        if (operators == null || operators.isEmpty()) {
            return "Pipeline {}";
        }
        String ops = operators.stream()
                .map(OperatorNode::toString)
                .collect(Collectors.joining(" | "));
        return "Pipeline { " + ops + " }";
    }

    // Represents a single operator node in the pipeline
    public record OperatorNode(
            Operator type,
            OperatorArguments arguments
    ) implements Serializable {
        @Override
        public String toString() {
            return String.format("%s(%s)", type.name().toLowerCase(), arguments.toString());
        }
    }

    public interface OperatorArguments extends Serializable {}

    // Represents a single logical condition for the filter operator
    public record FilterArgs(
            String variable,
            Condition condition,
            double value
    ) implements OperatorArguments {

        @Override
        public String toString() {
            String opString = switch(condition) {
                case LESS_THAN -> "<";
                case GREATER_THAN -> ">";
                case LESS_OR_EQUAL -> "<=";
                case GREATER_OR_EQUAL -> ">=";
            };

            return String.format("%s %s %.4f", variable, opString, value);
        }
    }

    // Represents the arguments (in this case just the probability) for the map operator that duplicates tuples
    public record MapDuplicateArgs(
            double probability
    ) implements OperatorArguments {
        @Override
        public String toString() {
            return String.format("probability=%.2f", probability);
        }
    }

    // Represents the arguments for the map operator that adds noise
    public record MapNoiseArgs(
            String attribute,
            double percentage
    )implements OperatorArguments {
        @Override
        public String toString() {
            return String.format("attribute=%s, percentage=%.2f", attribute, percentage);
        }
    }
}