package jgea.mappers;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

// Pipeline Representation (phenotype)
public record QueryRepresentation(
        // Contains a sequence of operators
        List<OperatorNode> operators
) implements Serializable {

    public enum Operator implements Serializable {
        LESS_THAN, GREATER_THAN, LESS_OR_EQUAL, GREATER_OR_EQUAL, EQUAL
    }

    public static Operator fromString(String text) {
        return switch (text) {
            case "lt" -> Operator.LESS_THAN;
            case "gt" -> Operator.GREATER_THAN;
            case "le" -> Operator.LESS_OR_EQUAL;
            case "ge" -> Operator.GREATER_OR_EQUAL;
            case "eq" -> Operator.EQUAL;
            default -> throw new IllegalArgumentException("Operatore non valido: " + text);
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

    // Represents a single operator node in the pipeline (in this case we only have filters)
    public record OperatorNode(
            String type, // Es. "FILTER", "AGGREGATE"
            Condition condition
    ) implements Serializable {
        @Override
        public String toString() {
            return String.format("%s(%s)", type.toLowerCase(), condition.toString());
        }
    }

    // Represents a single logical condition ("coLevel > 2.5")
    public record Condition(
            String variable,
            Operator operator,
            Object value
    ) implements Serializable {

        @Override
        public String toString() {
            String opString = switch(operator) {
                case LESS_THAN -> "<";
                case GREATER_THAN -> ">";
                case LESS_OR_EQUAL -> "<=";
                case GREATER_OR_EQUAL -> ">=";
                case EQUAL -> "==";
            };

            String valueString = (value instanceof Double) ? String.format("%.4f", value) : value.toString();
            return String.format("%s %s %s", variable, opString, valueString);
        }
    }
}