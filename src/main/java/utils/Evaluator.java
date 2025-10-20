package utils;

import evaluation.Sequence;
import event.AirQualityEvent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    // Helper method to extract the tuple ID from an event
    private static long parseIdFromEvent(String eventString) {
        try {
            String idTag = "tupleID=";
            int startIndex = eventString.indexOf(idTag) + idTag.length();
            int endIndex = eventString.indexOf(",", startIndex);
            if (endIndex == -1) {
                endIndex = eventString.indexOf("}", startIndex);
            }
            String idStr = eventString.substring(startIndex, endIndex);
            return Long.parseLong(idStr);
        } catch (Exception e) {
            System.err.println("Error parsing ID from event string: " + eventString);
            return -1;
        }
    }

    // Convert the output from a Flink CEP query into a List of Sequence
    public static List<Sequence> parseSequencesFromEvents(List<List<AirQualityEvent>> cepResultEvents) {

        if (cepResultEvents == null) {
            return new ArrayList<>();
        }

        return cepResultEvents.stream()
                .map(eventList -> {
                    // For each list of events (a sequence) extract a list of tuple ID
                    List<Long> tupleIds = eventList.stream()
                            .map(AirQualityEvent::getTupleId)
                            .collect(Collectors.toList());
                    // Create the sequence
                    return new Sequence(tupleIds);
                })
                .collect(Collectors.toList());
    }

}
