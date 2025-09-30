package utils;

import evaluation.Sequence;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class Evaluator {

    // Read from CSV file and parse each line in a Sequence creating a list of objects Sequence
    public static List<Sequence> parseSequencesFromFile(Path filePath) throws IOException {
        List<Sequence> sequences = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                // Divide the line in different events using '|' as delimiter
                String[] events = line.split("\\|");
                if (events.length > 0) {
                    // Extract the hour of the first and last event in the sequence to calculate the start and end time of the sequence
                    LocalDateTime startTime = parseDateTimeFromEvent(events[0]);
                    LocalDateTime lastEventTime = parseDateTimeFromEvent(events[events.length - 1]);
                    LocalDateTime endTime = (lastEventTime != null) ? lastEventTime.plusHours(1) : null;

                    if (startTime != null && endTime != null) {
                        sequences.add(new Sequence(startTime, endTime));
                    }
                }
            }
        }
        return sequences;
    }

    // Helper to extract the LocalDateTime from an event
    private static LocalDateTime parseDateTimeFromEvent(String eventString) {
        try {
            int startIndex = eventString.indexOf("=") + 1;
            int endIndex = eventString.indexOf(",");
            String dateTimeStr = eventString.substring(startIndex, endIndex);
            return LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (Exception e) {
            System.err.println("Errore nel parsing dell'evento: " + eventString);
            return null;
        }
    }
}
