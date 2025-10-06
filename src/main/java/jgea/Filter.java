package jgea;

public class Filter {

    public enum Operator{
        GREATER_THAN,       //gt
        LESS_THAN,          //lt
        LESS_OR_EQUAL,      // le
        GREATER_OR_EQUAL,   // ge
        EQUAL               // eq
    }
    private final String attribute;
    private final double threshold;
    private final Operator operator;

    public Filter(String attribute, double threshold, Operator operator) {
        this.attribute = attribute;
        this.threshold = threshold;
        this.operator = operator;
    }

    public String getAttribute() {return attribute;}

    public double getThreshold() {return threshold;}

    public Operator getOperator() {return operator;}

    @Override
    public String toString() {
        return String.format("Filter[%s %s %.2f]", attribute, operator.name(), threshold);
    }

}
