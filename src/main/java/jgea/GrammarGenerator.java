package jgea;

import jgea.utils.CSVAnalyzer.AttributeStats;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

// Generate a grammar to define operators like filters as strings and save the grammar in a file
public class GrammarGenerator {

    private static final int DECIMAL_PRECISION_DIGITS = 4;

    public static void generateGrammar(List<String> attributes, Map<String, AttributeStats> statsMap, String filePath) {
        if (statsMap == null || statsMap.isEmpty()) {
            throw new IllegalArgumentException("Cannot generate grammar: stats map is empty or null.");
        }

        StringBuilder sb = new StringBuilder();

        sb.append("<pipeline> ::= <filter> | <filter> <pipeline>\n");
        sb.append("<filter> ::= <attribute> <operator> <value>\n");

        sb.append("<attribute> ::= ");
        StringJoiner attrJoiner = new StringJoiner(" | ");
        for (String attribute : attributes) {
            attrJoiner.add("'" + attribute + "'");
        }
        sb.append(attrJoiner).append("\n");

        sb.append("<operator> ::= lt | le | gt | ge | eq\n");

        // --- Selettore valore ---
        sb.append("<value> ::= ");
        StringJoiner valueJoiner = new StringJoiner(" | ");
        for (String attribute : attributes) {
            String cleanAttr = attribute.replaceAll("[^a-zA-Z0-9_]", "_");
            valueJoiner.add("<" + cleanAttr + "_value>");
        }
        sb.append(valueJoiner).append("\n");

        // Specific attribute rules
        for (String attribute : attributes) {
            AttributeStats stats = statsMap.get(attribute);
            if (stats == null) continue;

            String cleanAttr = attribute.replaceAll("[^a-zA-Z0-9_]", "_");

            sb.append(String.format(
                    "<%s_value> ::= <%s_intPart> . <%s_fracPart>\n",
                    cleanAttr, cleanAttr, cleanAttr
            ));

            generateIntegerRule(sb, "<" + cleanAttr + "_intPart>", stats);
            generateFixedFractionRule(sb, "<" + cleanAttr + "_fracPart>");
        }

        sb.append("<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");
        sb.append("<non_zero_digit> ::= 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");

        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write(sb.toString());
            System.out.println("Grammar generated successfully");
        } catch (IOException e) {
            System.err.println("Error writing grammar");
        }
    }

    // Generate a rule for the integer part with a number of digits between minIntDigits and maxIntDigits
    private static void generateIntegerRule(StringBuilder sb, String ruleName, AttributeStats stats) {
        sb.append(ruleName).append(" ::= ");
        StringJoiner options = new StringJoiner(" | ");

        for (int i = stats.minIntDigits(); i <= stats.maxIntDigits(); i++) {
            StringJoiner digits = new StringJoiner(" ");
            if (i > 1) {
                digits.add("<non_zero_digit>");
                for (int j = 1; j < i; j++) digits.add("<digit>");
            } else {
                digits.add("<digit>");
            }
            options.add(digits.toString());
        }

        sb.append(options).append("\n");
    }

    // Generate a rule for the fixed fractional part (4 digits)
    private static void generateFixedFractionRule(StringBuilder sb, String ruleName) {
        sb.append(ruleName).append(" ::= ");
        StringJoiner digits = new StringJoiner(" ");
        for (int i = 0; i < DECIMAL_PRECISION_DIGITS; i++) {
            digits.add("<digit>");
        }
        sb.append(digits).append("\n");
    }
}
