package anonymization;

import common.util.Util;
import component.sink.Sink;
import component.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import query.Query;
import component.operator.Operator;
import event.AirQualityEvent;

public class AnonymizationQuery {

    private static final Logger logger = LoggerFactory.getLogger(AnonymizationQuery.class);

    public static void main(String[] args) {

        final String inputFile = AnonymizationQuery.class.getClassLoader().getResource("datasets/airQuality.csv").getPath();

        Query anonymizationQuery = new Query();

        // Source from CSV file
        Source<String> inputSource = anonymizationQuery.addTextFileSource("I1", inputFile);

        // Operator to read and parse the line
        Operator<String, AirQualityEvent> inputReader =
                anonymizationQuery.addMapOperator(
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
                anonymizationQuery.addFilterOperator(
                        "filter",
                        tuple -> {
                            boolean keep = tuple.getCoLevel() >= 2.0;
                            return keep;
                        });

        // Window of 2 hours
        long windowSize = 2 * 60 * 60 * 1000;

        // Operator to replace the colevel with the average between the value and the level in the previous 2 hours
        AverageMap averageFunction = new AverageMap(windowSize);
        Operator<AirQualityEvent, AirQualityEvent> movingAverageMapOperator =
                anonymizationQuery.addMapOperator("average", tuple -> averageFunction.apply(tuple));

        // Finale sink to print in a CSV file
        Sink<AirQualityEvent> outputSink =
                anonymizationQuery.addTextFileSink("o1", "src/main/resources/datasets/anonymizedDataset.csv", true);

        anonymizationQuery.connect(inputSource, inputReader)
                .connect(inputReader, filter)
                .connect(filter, movingAverageMapOperator)
                .connect(movingAverageMapOperator, outputSink);

        logger.info("*** Anonymization query activated ***");
        anonymizationQuery.activate();
    }
}
