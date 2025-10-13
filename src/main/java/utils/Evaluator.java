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
                List<Long> idsInSequence = new ArrayList<>();

                for (String eventString : events) {
                    // Extract the tuple ID from each event
                    long id = parseIdFromEvent(eventString);
                    if (id != -1) {
                        idsInSequence.add(id);
                    }
                }

                if (!idsInSequence.isEmpty()) {
                    sequences.add(new Sequence(idsInSequence));
                }
            }
        }
        return sequences;
    }

    // In utils/Evaluator.java
    private static long parseIdFromEvent(String eventString) {
        try {
            // Cerca la stringa "tupleId="
            String idTag = "tupleId=";
            int startIndex = eventString.indexOf(idTag) + idTag.length();
            int endIndex = eventString.indexOf(",", startIndex); // Trova la virgola successiva
            if (endIndex == -1) {
                endIndex = eventString.indexOf("}", startIndex); // Se è l'ultimo elemento
            }
            String idStr = eventString.substring(startIndex, endIndex);
            return Long.parseLong(idStr);
        } catch (Exception e) {
            System.err.println("Error parsing ID from event string: " + eventString);
            return -1;
        }
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
