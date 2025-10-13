package jgea.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class CSVAnalyzer {

    public record AttributeStats(double min, double max, int minIntDigits, int maxIntDigits) {}

    // Helper method to read the CSV file in the package resources
    private static BufferedReader getReaderForResource(String resourcePath) throws IOException {
        InputStream in = CSVAnalyzer.class.getClassLoader().getResourceAsStream(resourcePath);
        if (in == null) throw new IOException("Resource not found in classpath: " + resourcePath);
        return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
    }

    // Read the first line of the CSV file and extract the column names
    public static List<String> extractAttributes(String resourcePath) throws IOException {
        try (BufferedReader reader = getReaderForResource(resourcePath)) {
            String header = reader.readLine();
            if (header == null) throw new IOException("CSV file is invalid or empty");

            return Arrays.stream(header.split(";"))
                    .map(String::trim) // Remove blank space
                    .filter(h -> !h.isEmpty() &&
                            !h.equalsIgnoreCase("ID") &&
                            !h.equalsIgnoreCase("Date") &&
                            !h.equalsIgnoreCase("Time"))
                    .collect(Collectors.toList());
        }
    }

    // Analyze the dataset and create a map with the attribute name and its stats (min, max, minDigits, maxDigits)
    public static Map<String, AttributeStats> analyze(String resourcePath) throws IOException {
        List<String> headersOfInterest = extractAttributes(resourcePath);

        try (BufferedReader reader = getReaderForResource(resourcePath)) {
            String headerLine = reader.readLine();
            if (headerLine == null) throw new IOException("CSV file is invalid or empty");

            String[] originalHeaders = headerLine.split(";");

            // Create a map to link a attribute name to its original column index
            Map<String, Integer> headerToIndex = new HashMap<>();
            for (int i = 0; i < originalHeaders.length; i++) {
                if (headersOfInterest.contains(originalHeaders[i].trim())) {
                    headerToIndex.put(originalHeaders[i].trim(), i);
                }
            }

            // Initialize the statistics map
            Map<String, AttributeStats> statsMap = headersOfInterest.stream()
                    .collect(Collectors.toMap(h -> h, h -> new AttributeStats(Double.MAX_VALUE, -Double.MAX_VALUE, Integer.MAX_VALUE, 0)));

            // Process each data row
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                String[] values = line.split(";");

                for (String header : headersOfInterest) {
                    Integer index = headerToIndex.get(header);
                    if (index == null || index >= values.length || values[index].trim().isEmpty()) continue;

                    String raw = values[index].trim().replace(',', '.');

                    try {
                        // Parse the value and update statistics
                        double value = Double.parseDouble(raw);
                        // Ignore missing value
                        if (value == -200.0) continue;

                        // Calculate the number of integer digits for the current value
                        String[] parts = raw.split("\\.");
                        String intPartString = parts[0].replace("-", "");
                        int intDigits = intPartString.isEmpty() ? 0 : intPartString.length();
                        if (intPartString.equals("0")) intDigits = 1;

                        AttributeStats current = statsMap.get(header);
                        statsMap.put(header, new AttributeStats(
                                Math.min(current.min(), value),
                                Math.max(current.max(), value),
                                Math.min(current.minIntDigits(), intDigits),
                                Math.max(current.maxIntDigits(), intDigits)
                        ));
                    } catch (NumberFormatException ignored) {
                        // If the value is not a number, ignore it
                    }
                }
            }
            return statsMap;
        }
    }
}