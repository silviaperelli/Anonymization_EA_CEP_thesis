import component.operator.in1.aggregate.BaseTimeWindowAddRemove;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import component.sink.Sink;
import component.source.Source;
import query.LiebreContext;
import query.Query;

import java.util.HashMap;
import java.util.function.Consumer;

import common.metrics.Metrics;
import common.metrics.MetricsFactory;
import component.operator.Operator;
import event.AirQualityEvent;

public class TestQueryLiebre {

    public static void main(String[] args) {

        final String inputFile = "src/main/resources/datasets/airQuality.csv";

        // You can define your own consumers for metrics collection
        HashMap<String, Consumer<Object[]>> consumers = new HashMap<>();
        // For each metric id (yes, you need to know the name) define the consumer that
        // does what you want
        // In this case it prints, but you can also store data if you want in a data
        // structure, for instance
        consumers.put("average_filter2.IN", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("average_filter2.OUT", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("filter1_average.IN", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("filter1_average.OUT", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("filter2_o1.IN", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("filter2_o1.OUT", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("I1_reader.IN", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("I1_reader.OUT", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("reader_filter1.IN", x -> System.out.println(x[0] + ", " + x[1]));
        consumers.put("reader_filter1.OUT", x -> System.out.println(x[0] + ", " + x[1]));

        // set metrics before any operators are added
        MetricsFactory metrics = Metrics.fileAndConsumer("src/main/resources", consumers);
        LiebreContext.mergeWithStreamMetrics(metrics);

        Query query = new Query();

        // Source from CSV file
        Source<String> inputSource = query.addTextFileSource("I1", inputFile);

        // Operator to read and parse the line
        Operator<String, AirQualityEvent> inputReader = query.addMapOperator(
                "reader", line -> {
                    if (line.startsWith("ID;Date;Time;CO(GT)")) {
                        return null;
                    }
                    return AirQualityEvent.eventCreation(line);
                });

        // Operator to filter tuple with CO level >= 2.0 and NO2 level >= 40.0
        Operator<AirQualityEvent, AirQualityEvent> filter1 = query.addFilterOperator(
                "filter1", tuple -> (tuple.getCoLevel() >= 2.0 && tuple.getNo2() >= 40.0));

        // Window of 3 hours
        final long WINDOW_SIZE = 3 * 60 * 60 * 1000;
        final long WINDOW_SLIDE = 60 * 60 * 1000;

        // Operator to aggregate the CO level and NO2 level in a window of 2 hours
        Operator<AirQualityEvent, AirQualityEvent> aggregateOperator = query.addTimeAggregateOperator("average",
                WINDOW_SIZE, WINDOW_SLIDE, new AggregateWindow());

        // Operator to filter tuple with aggregate CO level >= 5.0 and aggregate NO2
        // level >= 100.0
        Operator<AirQualityEvent, AirQualityEvent> filter2 = query.addFilterOperator(
                "filter2", tuple -> (tuple.getCoLevel() >= 5.0 && tuple.getNo2() >= 100.0));

        // Finale sink to print in a CSV file
        Sink<AirQualityEvent> outputSink = query.addTextFileSink("o1", "src/main/resources/resultsTestQuery.csv", true);

        // Sink<AirQualityEvent> outputSink = query.addSink(new MyBaseSink("o1", new TextFileSinkFunction<>("src/main/resources/resultsTestQuery.csv", true)));

        query.connect(inputSource, inputReader).connect(inputReader, filter1).connect(filter1, aggregateOperator)
                .connect(aggregateOperator, filter2).connect(filter2, outputSink);

        query.activate();
        System.out.println("*** Anonymization query activated ***");
        while(outputSink.isEnabled()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("*** Anonymization query completed ***");

        LiebreContext.unmergeFromStreamMetrics(metrics);

    }

    private static class AggregateWindow extends BaseTimeWindowAddRemove<AirQualityEvent, AirQualityEvent> {

        private int count = 0;
        private double sumCO = 0.0;
        private double sumNO2 = 0.0;
        private AirQualityEvent lastEvent = null;
        private long lastOutputTs = -1L;

        @Override
        public void add(AirQualityEvent event) {
            if (!Double.isNaN(event.getCoLevel()) && !Double.isNaN(event.getNo2())) {
                sumCO += event.getCoLevel();
                sumNO2 += event.getNo2();
                count++;
                lastEvent = event;
            }
        }

        @Override
        public void remove(AirQualityEvent event) {
            if (!Double.isNaN(event.getCoLevel()) && !Double.isNaN(event.getNo2())) {
                sumCO -= event.getCoLevel();
                sumNO2 -= event.getNo2();
                count--;
            }
        }

        @Override
        public AirQualityEvent getAggregatedResult() {
            if (count == 0 || lastEvent == null) {
                return AirQualityEvent.createEmptyEvent(this.startTimestamp);
            }

            // Avoid duplicates due to the previous filter operator in the pipeline
            // If the last event in the window is the same as the one in the last output,
            // ignore it
            if (lastEvent.getTimestamp() == lastOutputTs) {
                return AirQualityEvent.createEmptyEvent(this.startTimestamp);
            }

            double averageCO = sumCO / count;
            double averageNO2 = sumNO2 / count;
            lastOutputTs = lastEvent.getTimestamp();
            return new AirQualityEvent(lastEvent, averageCO, averageNO2);
        }

        @Override
        public TimeWindowAddRemove<AirQualityEvent, AirQualityEvent> factory() {
            return new AggregateWindow();
        }

    }
}
