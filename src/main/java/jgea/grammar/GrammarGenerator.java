package jgea.grammar;

import jgea.grammar.utils.CSVAnalyzer;
import jgea.grammar.utils.CSVAnalyzer.AttributeStats;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class GrammarGenerator {

    private static final int DECIMAL_PRECISION_DIGITS = 1;

    public static void main(String[] args) throws IOException {
        final String grammarPath = "src/main/resources/generated-grammar.bnf";
        final String csvPath = "datasets/airQuality.csv";
        // Extract attributes and their numerical bounds from a CSV file
        List<String> attributes = CSVAnalyzer.extractAttributes(csvPath);
        Map<String, CSVAnalyzer.AttributeStats> statsMap = CSVAnalyzer.analyze(csvPath);
        // Grammar generation
        generateGrammar(attributes, statsMap, grammarPath);
    }

    // Generate a grammar to define operators like filters as strings and save the grammar in a file
    public static void generateGrammar(List<String> attributes, Map<String, AttributeStats> statsMap, String filePath) {
        if (statsMap == null || statsMap.isEmpty()) {
            throw new IllegalArgumentException("Cannot generate grammar: stats map is empty or null.");
        }

        StringBuilder sb = new StringBuilder();

        sb.append("<pipeline> ::= <operator> | <operator> <pipeline>\n");
        sb.append("<operator> ::= <filter> | <map_duplicate> | <map_noise>\n");

        // Operator definition
        sb.append("<filter> ::= <attribute> <condition> <value>\n");
        sb.append("<map_duplicate> ::= <probability>\n");
        sb.append("<map_noise> ::= <attribute> <percentage>\n");

        sb.append("<attribute> ::= ");
        StringJoiner attrJoiner = new StringJoiner(" | ");
        for (String attribute : attributes) {
            attrJoiner.add("'" + attribute + "'");
        }
        sb.append(attrJoiner).append("\n");

        sb.append("<condition> ::= lt | le | gt | ge\n");

        sb.append("<value> ::= ");
        StringJoiner valueJoiner = new StringJoiner(" | ");
        for (String attribute : attributes) {
            String cleanAttr = cleanAttribute(attribute);
            valueJoiner.add("<" + cleanAttr + "_value>");
        }
        sb.append(valueJoiner).append("\n");

        // Specific attribute rules for the filter value
        for (String attribute : attributes) {
            AttributeStats stats = statsMap.get(attribute);
            if (stats == null) continue;

            String cleanAttr = cleanAttribute(attribute);

            sb.append(String.format(
                    "<%s_value> ::= <%s_intPart> . <%s_fracPart>\n",
                    cleanAttr, cleanAttr, cleanAttr
            ));

            generateIntegerRule(sb, "<" + cleanAttr + "_intPart>", stats);
            generateFixedFractionRule(sb, "<" + cleanAttr + "_fracPart>");
        }

        sb.append("<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");
        sb.append("<non_zero_digit> ::= 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");

        sb.append("<probability> ::= 0.1 | 0.2 | 0.3 | 0.4 | 0.5 | 0.6 | 0.7 | 0.8 | 0.9 | 1.0\n");
        sb.append("<percentage> ::= 0.01 | 0.05 | 0.10 | 0.25\n");

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

    // Helper method to clean an attribute name
    private static String cleanAttribute(String attributeName) {
        // Replace invalid character with an underscore
        String cleaned = attributeName.replaceAll("[^a-zA-Z0-9]+", "_");
        // Remove underscore at the end or at the beginning of the string
        return cleaned.replaceAll("^_+|_+$", "");
    }
}
