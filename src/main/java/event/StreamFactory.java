package event;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.net.URL;
import java.util.Objects;

public class StreamFactory {
    // Creates a DataDtream by reading from the CSV file in resources

    public static DataStream<AirQualityEvent> createStream(StreamExecutionEnvironment env, String filePath) {
        //Looks for the CSV file in the directory resources
        URL fileURL = StreamFactory.class.getClassLoader().getResource(filePath);
        if(fileURL == null){
            throw new RuntimeException("File not found");
        }

        DataStream<AirQualityEvent> dataStream = env.readTextFile(fileURL.getPath())
                .filter(line -> !line.startsWith("Date;Time;CO(GT)")) // Skips the header
                .map(AirQualityEvent::eventCreation) // Creation of an event from a row
                .filter(Objects::nonNull); // Skips invalid line

        // Assign watermark
        return dataStream.assignTimestampsAndWatermarks(new AirQualityWatermarkStrategy());
    }

    // Class to extract timestamp and generate watermarks
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

                // Emits a watermark based on the max timestamp seen
                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(maxTimestampSeen));
                }
            };
        }
    }
}
