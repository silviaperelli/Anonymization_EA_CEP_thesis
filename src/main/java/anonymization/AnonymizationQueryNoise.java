package anonymization;

import common.util.Util;
import component.operator.Operator;
import component.sink.Sink;
import component.source.Source;
import event.AirQualityEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import query.Query;

public class AnonymizationQueryNoise {

    private static final Logger logger = LoggerFactory.getLogger(AnonymizationQueryNoise.class);

    public static void main(String[] args) {

        final String inputFile = AnonymizationQueryNoise.class.getClassLoader().getResource("datasets/airQuality.csv").getPath();

        Query q = new Query();

        // Source from CSV file
        Source<String> inputSource = q.addTextFileSource("I1", inputFile);

        // Operator to read and parse the line
        Operator<String, AirQualityEvent> inputReader =
                q.addMapOperator(
                        "reader",
                        line -> {
                            Util.sleep(100);
                            if (line.startsWith("Date;Time;CO(GT)")) {
                                return null;
                            }
                            return AirQualityEvent.eventCreation(line);
                        });

        // Operator to filter tuple with COLevel >= 2.0
        Operator<AirQualityEvent, AirQualityEvent> filter =
                q.addFilterOperator(
                        "filter",
                        tuple -> {
                            boolean keep = tuple.getCoLevel() >= 2.0;
                            return keep;
                        });

        // Window of 2 hours
        double stdDev = 0.5;

        // Operator to replace the colevel with the average between the value and the level in the previous 2 hours
        NoiseOperator noiseFunction = new NoiseOperator(stdDev);
        Operator<AirQualityEvent, AirQualityEvent> noiseMap =
                q.addMapOperator("noise", tuple -> noiseFunction.apply(tuple));

        // Finale sink to print in a CSV file
        Sink<AirQualityEvent> outputSink =
                q.addTextFileSink("o1", "src/main/resources/datasets/anonymizedDatasetNoise.csv", true);

        q.connect(inputSource, inputReader)
                .connect(inputReader, filter)
                .connect(filter, noiseMap)
                .connect(noiseMap, outputSink);

        logger.info("*** Anonymization query activated ***");
        q.activate();
    }
}
