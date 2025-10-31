package cep;

import event.AirQualityEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import java.util.*;

public class PollutionAlertQuery {

    // Create pattern to detect high CO Pattern
    public static Pattern<AirQualityEvent, ?> createHighCoPattern() {
        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipToFirst("end");

        // Define the CEP Pattern
        Pattern<AirQualityEvent, ?> highCO = Pattern.<AirQualityEvent>begin("start", skipStrategy)
                .where(new SimpleCondition<AirQualityEvent>() {
                    @Override
                    public boolean filter(AirQualityEvent airQualityEvent) {
                        return airQualityEvent.getCoLevel() > 5.0;
                    }
                })
                // Search for one or more consecutive occurrences
                .oneOrMore().consecutive()
                // The sequence ends when a tuple has COLevel <= 5.0
                .followedBy("end")
                .where(new SimpleCondition<AirQualityEvent>() {
                    @Override
                    public boolean filter(AirQualityEvent airQualityEvent) {
                        return airQualityEvent.getCoLevel() <= 5.0;
                    }
                });
        return highCO;
    }

    // Apply the pattern to the stream and return the sequence of alert tuples
    public static List<List<AirQualityEvent>> processAlerts(DataStream<AirQualityEvent> eventStream) throws Exception {

        Pattern<AirQualityEvent, ?> highCOPattern = createHighCoPattern();

        // Apply the pattern and selecting the results
        DataStream<List<AirQualityEvent>> alertStream = CEP.pattern(eventStream, highCOPattern)
                .select(new PatternSelectFunction<AirQualityEvent, List<AirQualityEvent>>() {
                    @Override
                    public List<AirQualityEvent> select(Map<String, List<AirQualityEvent>> pattern) {
                        List<AirQualityEvent> highCOEvents = pattern.get("start");

                        //Minimum duration must be 2 hours
                        if (highCOEvents != null && highCOEvents.size() >= 2) {
                            return highCOEvents;
                        }
                        return null;
                    }
                })
                .filter(Objects::nonNull); // Skip matches that did not generate an alert (< 2 hours)

        Iterator<List<AirQualityEvent>> alertsIterator = alertStream.executeAndCollect();

        // Return the list of all sequences found
        List<List<AirQualityEvent>> results = new ArrayList<>();
        while (alertsIterator.hasNext()) {
            results.add(alertsIterator.next());
        }

        return results;
    }


    // Utility method to log a detected sequence
    public static String formatAlertInfo(List<AirQualityEvent> sequence) {
        AirQualityEvent firstEvent = sequence.get(0);
        double avgCO = sequence.stream()
                .mapToDouble(AirQualityEvent::getCoLevel)
                .average()
                .orElse(0.0);

        return String.format(
                "*** ALERT: CO pollution episode detected! ***\n" +
                        "\t- Start: %s\n" +
                        "\t- Duration: %d hours\n" +
                        "\t- Average CO Level: %.2f mg/m^3\n",
                firstEvent.getEventTime(),
                sequence.size(),
                avgCO
        );
    }
}