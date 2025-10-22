package event;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.Evaluator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.net.URL;
import java.util.stream.Collectors;

public class StreamFactory {

    // Create a DataDtream by reading from the CSV file in resources
    public static DataStream<AirQualityEvent> createStreamfromFile(StreamExecutionEnvironment env, String filePath) {

        URL fileURL = StreamFactory.class.getClassLoader().getResource(filePath);
        if(fileURL == null){
            throw new RuntimeException("File not found");
        }

        DataStream<AirQualityEvent> dataStream = env.readTextFile(fileURL.getPath())
                .filter(line -> !line.startsWith("ID;Date;Time;CO(GT)")) // Skip the header
                .map(AirQualityEvent::eventCreation) // Create an event from a line
                .filter(Objects::nonNull); // Skip invalid line

        // Assign watermark
        return dataStream.assignTimestampsAndWatermarks(new AirQualityWatermarkStrategy());
    }

    // Create a DataStream from a collection in memory
    public static DataStream<AirQualityEvent> createStreamFromCollection(StreamExecutionEnvironment env, List<AirQualityEvent> events) {
        return env.fromCollection(events).assignTimestampsAndWatermarks(new AirQualityWatermarkStrategy());
    }


// Load and parse the CSV file into a list of AirQuality Events
    public static List<AirQualityEvent> createListFromFile(String resourcePath) throws IOException {
        InputStream inputStream = Evaluator.class.getClassLoader().getResourceAsStream(resourcePath);
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

    // Extract timestamp and generate watermarks
    private static class AirQualityWatermarkStrategy implements WatermarkStrategy<AirQualityEvent> {

        @Override
        public TimestampAssigner<AirQualityEvent> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (airQualityEvent, recordTimestamp) -> airQualityEvent.getTimestamp();
        }

        @Override
        public WatermarkGenerator<AirQualityEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<AirQualityEvent>() {
                private long maxTimestampSeen = Long.MIN_VALUE;

                // Update the max timestamp seen so far
                @Override
                public void onEvent(AirQualityEvent airQualityEvent, long eventTimestamp, WatermarkOutput output) {
                    maxTimestampSeen = Math.max(maxTimestampSeen, eventTimestamp);
                }

                // Emit a watermark based on the max timestamp seen
                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(maxTimestampSeen));
                }
            };
        }
    }
}
