package event;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.Objects;
import java.net.URL;

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
