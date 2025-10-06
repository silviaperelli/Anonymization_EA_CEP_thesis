package jgea;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.StringJoiner;

// Generate a grammar to define operators like filters as strings and save the grammar in a file
public class GrammarGenerator {
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

        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(sb.toString());
        } catch (IOException e) {
            System.err.println("Error writing the grammar: " + e.getMessage());
        }
    }
}