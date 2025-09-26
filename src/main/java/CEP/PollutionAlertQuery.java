package CEP;

import event.AirQualityEvent;
import event.StreamFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PollutionAlertQuery {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<AirQualityEvent> eventStream = StreamFactory.createStream(env, "dataset/AirQuality.csv");

        // Definition of CEP Pattern
        Pattern<AirQualityEvent, ?> highCO = Pattern.<AirQualityEvent>begin("start")
                .where(new SimpleCondition<AirQualityEvent>() {
                    @Override
                    public boolean filter(AirQualityEvent airQualityEvent) {
                        return airQualityEvent.getCoLevel() > 5.0;
                    }
                })
                // Search for one or more consecutive occurrences
                .oneOrMore().consecutive();


        // Applying the pattern and selecting the results
        DataStream<String> alertStream = CEP.pattern(eventStream, highCO)
                .select(new PatternSelectFunction<AirQualityEvent, String>() {
                    @Override
                    public String select(Map<String, List<AirQualityEvent>> pattern){
                        List<AirQualityEvent> highCOEvents = pattern.get("start");

                        //Minimum duration must be 2 hours
                        if(highCOEvents != null && highCOEvents.size() >= 2){
                            AirQualityEvent firstEvent = highCOEvents.get(0);
                            double avgCO = highCOEvents.stream().mapToDouble(AirQualityEvent::getCoLevel).average().orElse(0.0);
                             return String.format(
                                     "*** ALLERT: CO2 pollution episode detected! ***\n" +
                                             "\t- Start: %s\n" +
                                             "\t- Duration: %d hours\n" +
                                             "\t- Average CO Level Medio CO: %.2f mg/m^3\n",
                                     firstEvent.getEventTime().toString(),
                                     highCOEvents.size(),
                                     avgCO
                             );
                        }
                        return null;
                    }
                })
                .filter(Objects::nonNull); // Skips matches that did not generate an alert (< 2 hours)

        // --- MODIFICA CHIAVE QUI ---
        System.out.println("Avvio del job Flink e raccolta dei risultati...");

        // 1. Il tipo di ritorno è Iterator<String>
        Iterator<String> alertsIterator = alertStream.executeAndCollect();

        System.out.println("--- Rilevamento Episodi di Inquinamento Completato ---");
        System.out.println("Risultati trovati:\n");

        // 2. Usiamo forEachRemaining per scorrere e stampare ogni elemento
        alertsIterator.forEachRemaining(System.out::println);
    }
}



