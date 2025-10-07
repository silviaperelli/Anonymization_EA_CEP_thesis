package jgea;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

// Generate a grammar to define operators like filters as strings and save the grammar in a file
public class GrammarGenerator {

    private static final int MAX_DIGITS = 4;

    public static void generateGrammar(List<String> attributes, String filePath) {
        StringBuilder sb = new StringBuilder();

        sb.append("<pipeline> ::= <filter> | <filter> <pipeline>\n");
        sb.append("<filter> ::= <attribute> <operator> <value>\n");

        sb.append("<attribute> ::= ");
        StringJoiner sj = new StringJoiner(" | ");
        for (String attribute : attributes) {
            sj.add("'" + attribute + "'");
        }
        sb.append(sj.toString()).append("\n");

        sb.append("<operator> ::= lt | le | gt | ge | eq\n");
        sb.append("<value> ::= <double>\n");
        sb.append("<double> ::= <intPart> | <intPart> . <fracPart>\n");

        // Create the integer part of the double value with a maximum of 4 digits
        sb.append("<intPart> ::= ");
        StringJoiner intPartJoiner = new StringJoiner(" | ");
        for (int i = 1; i <= MAX_DIGITS; i++) {
            StringJoiner digitSequence = new StringJoiner(" ");
            for (int j = 0; j < i; j++) {
                digitSequence.add("<digit>");
            }
            intPartJoiner.add(digitSequence.toString());
        }
        sb.append(intPartJoiner.toString()).append("\n");

        // Create the fractional part of the double value with a maximum of 4 digits
        sb.append("<fracPart> ::= ");
        StringJoiner fracPartJoiner = new StringJoiner(" | ");
        for (int i = 1; i <= MAX_DIGITS; i++) {
            StringJoiner digitSequence = new StringJoiner(" ");
            for (int j = 0; j < i; j++) {
                digitSequence.add("<digit>");
            }
            fracPartJoiner.add(digitSequence.toString());
        }
        sb.append(fracPartJoiner.toString()).append("\n");

        sb.append("<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(sb.toString());
        } catch (IOException e) {
            System.err.println("Error writing the grammar: " + e.getMessage());
        }
    }
}