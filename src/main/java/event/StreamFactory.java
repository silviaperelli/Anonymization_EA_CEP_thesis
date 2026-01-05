package event;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class StreamFactory {

// Load and parse the CSV file into a list of AirQuality Events
    public static List<AirQualityEvent> createListFromFile(String resourcePath) throws IOException {
        InputStream inputStream = StreamFactory.class.getClassLoader().getResourceAsStream(resourcePath);
        if (inputStream == null) {
            throw new IOException("Resource not found in classpath: " + resourcePath);
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            return reader.lines()
                    .skip(1) // Skip the first line (header)
                    .map(AirQualityEvent::eventCreation) // Create an event from a line
                    .filter(Objects::nonNull) // Skip invalid line
                    .collect(Collectors.toList()); // Collect the events into a list
        }
    }
}
