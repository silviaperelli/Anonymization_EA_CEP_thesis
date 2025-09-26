package cep;

import event.AirQualityEvent;
import event.StreamFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollutionAlertQuery {

    private static final Logger logger = LoggerFactory.getLogger(PollutionAlertQuery.class);
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<AirQualityEvent> eventStream = StreamFactory.createStream(env, "datasets/airQuality.csv");
        Pattern<AirQualityEvent, ?> pollutionPattern = createHighCoPattern();
        findAndProcessAlerts(eventStream, pollutionPattern);
    }


    public static Pattern<AirQualityEvent, ?> createHighCoPattern() {
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
        return highCO;
    }

    public static void findAndProcessAlerts(DataStream<AirQualityEvent> eventStream, Pattern<AirQualityEvent, ?> highCOPattern) throws Exception {
        // Applying the pattern and selecting the results
        DataStream<List<AirQualityEvent>> alertStream = CEP.pattern(eventStream, highCOPattern)
                .select(new PatternSelectFunction<AirQualityEvent, List<AirQualityEvent>>() {
                    @Override
                    public List<AirQualityEvent> select(Map<String, List<AirQualityEvent>> pattern){
                        List<AirQualityEvent> highCOEvents = pattern.get("start");

                        //Minimum duration must be 2 hours
                        if(highCOEvents != null && highCOEvents.size() >= 2){
                            return highCOEvents;
                        }
                        return null;
                    }
                })
                .filter(Objects::nonNull); // Skips matches that did not generate an alert (< 2 hours)

        Iterator<List<AirQualityEvent>> alertsIterator = alertStream.executeAndCollect();

        if (!alertsIterator.hasNext()) {
            logger.info("No pollution sequences detected.");
        }else{
            logger.info("Target sequence detected:\n");
        }

        // Prepare output file
        String outputDir = "src/main/resources/datasets/target";
        String outputFilePath = outputDir + "/targetDataset.csv";
        new File(outputDir).mkdirs();

        int sequenceCount = 0;
        try (FileWriter writer = new FileWriter(outputFilePath)) {

            while(alertsIterator.hasNext()) {
                List<AirQualityEvent> sequence = alertsIterator.next();

                // Format and write the sequence on the file
                String rawSequenceLine = sequence.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining("|"));
                writer.write(rawSequenceLine + "\n");

                // Print the alert on Console
                AirQualityEvent firstEvent = sequence.get(0);
                double avgCO = sequence.stream().mapToDouble(AirQualityEvent::getCoLevel).average().orElse(0.0);
                String alertString = String.format(
                        "*** ALLERT: CO2 pollution episode detected! ***\n" +
                                "\t- Start: %s\n" +
                                "\t- Duration: %d hours\n" +
                                "\t- Average CO Level Medio CO: %.2f mg/m^3\n",
                        firstEvent.getEventTime().toString(),
                        sequence.size(),
                        avgCO
                );
                logger.info(alertString);
                sequenceCount++;
            }
        } catch (IOException e) {
            logger.error("Error writing output file: {}", outputFilePath, e);
        }

        logger.info("Found and saved {} sequence in the file {}.\n", sequenceCount, outputFilePath);
    }
}