package jgea.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// Read the first line of the CSV file and extract the column names
public class CSVHeaderReader {

    public static List<String> extractAttributes(String filePath) throws IOException {
        try(InputStream in = CSVHeaderReader.class.getClassLoader().getResourceAsStream(filePath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(in), StandardCharsets.UTF_8))) {

            String header = reader.readLine();
            if (header == null) {
                throw new IOException("CSV file is invalid");
            }

            String[] split = header.split(";");

            return Arrays.stream(split)
                    .map(String::trim) // Remove blank space
                    .filter(line -> !line.isEmpty()) // Remove empty string
                    .filter(line -> !line.equalsIgnoreCase("Date") && !line.equalsIgnoreCase("Time"))
                    .map(line -> line.replaceAll("\\(.*?\\)", "")) // Remove "(GT)", "(CO)", etc.
                    .collect(Collectors.toList());
        }
    }
}
