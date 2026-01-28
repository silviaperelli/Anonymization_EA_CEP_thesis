package jgea.grammar;

import jgea.grammar.utils.CSVAnalyzer;
import jgea.grammar.utils.CSVAnalyzer.AttributeStats;
import jgea.grammar.utils.GrammarUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class GrammarGeneratorMap {

    private static final Logger logger = LoggerFactory.getLogger(GrammarGeneratorMap.class);

    public static void main(String[] args) throws IOException {

        final String grammarPath = "src/main/resources/grammars/airQuality/airQuality_generated-grammar-map.bnf";
        final String csvPath = "datasets/airQuality_withSensorID.csv";
        final String keyColumn = "SensorID";

        List<String> excludedColumns = new ArrayList<>(List.of("timestamp", "ID"));
        if (keyColumn != null && !keyColumn.isEmpty()) {
            excludedColumns.add(keyColumn);
        }

        // Extract attributes and their numerical bounds from a CSV file
        List<String> attributes = CSVAnalyzer.extractAttributes(csvPath, excludedColumns);
        Map<String, CSVAnalyzer.AttributeStats> statsMap = CSVAnalyzer.analyze(csvPath, attributes);
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
            String cleanAttr = GrammarUtils.cleanAttribute(attribute);
            valueJoiner.add("<" + cleanAttr + "_value>");
        }
        sb.append(valueJoiner).append("\n");

        // Specific attribute rules for the filter value
        for (String attribute : attributes) {
            AttributeStats stats = statsMap.get(attribute);
            if (stats == null) continue;

            String cleanAttr = GrammarUtils.cleanAttribute(attribute);

            sb.append(String.format(
                    "<%s_value> ::= <%s_intPart> . <%s_fracPart>\n",
                    cleanAttr, cleanAttr, cleanAttr
            ));

            GrammarUtils.generateIntegerRule(sb, "<" + cleanAttr + "_intPart>", stats);
            GrammarUtils.generateFixedFractionRule(sb, "<" + cleanAttr + "_fracPart>");
        }

        sb.append("<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");
        sb.append("<non_zero_digit> ::= 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9\n");

        sb.append("<probability> ::= 0.1 | 0.2 | 0.3 | 0.4 | 0.5 | 0.6 | 0.7 | 0.8 | 0.9 | 1.0\n");
        sb.append("<percentage> ::= 0.01 | 0.05 | 0.10 | 0.25\n");

        try (FileWriter fw = new FileWriter(filePath)) {
            fw.write(sb.toString());
            logger.info("Grammar generated successfully: {}", filePath);
        } catch (IOException e) {
            logger.error("Error writing grammar to {}", filePath, e);
            throw new RuntimeException(e);
        }
    }
}
