import component.operator.in1.aggregate.BaseTimeWindowAddRemove;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import component.sink.Sink;
import component.source.Source;
import query.Query;
import component.operator.Operator;
import event.AirQualityEvent;

public class TestQueryLiebre {

    public static void main(String[] args) {

        final String inputFile = "src/main/resources/datasets/airQuality.csv";

        Query anonymizationQuery = new Query();

        // Source from CSV file
        Source<String> inputSource = anonymizationQuery.addTextFileSource("I1", inputFile);

        // Operator to read and parse the line
        Operator<String, AirQualityEvent> inputReader = anonymizationQuery.addMapOperator(
                "reader", line -> {
                    if (line.startsWith("ID;Date;Time;CO(GT)")) {
                        return null;
                    }
                    return AirQualityEvent.eventCreation(line);
                });

        // Operator to filter tuple with CO level >= 2.0 and NO2 level >= 40.0
        Operator<AirQualityEvent, AirQualityEvent> filter1 = anonymizationQuery.addFilterOperator(
                "filter1", tuple -> (tuple.getCoLevel() >= 2.0 && tuple.getNo2() >= 40.0));

        // Window of 2 hours
        final long WINDOW_SIZE = 3 * 60 * 60 * 1000;
        final long WINDOW_SLIDE = 60 * 60 * 1000;

        // Operator to aggregate the CO level and NO2 level in a window of 2 hours
        Operator<AirQualityEvent, AirQualityEvent> aggregateOperator = anonymizationQuery.addTimeAggregateOperator("average", WINDOW_SIZE, WINDOW_SLIDE, new AggregateWindow());

        // Operator to filter tuple with aggregate CO level >= 5.0 and aggregate NO2 level >= 100.0
        Operator<AirQualityEvent, AirQualityEvent> filter2 = anonymizationQuery.addFilterOperator(
                "filter2", tuple -> (tuple.getCoLevel() >= 5.0 && tuple.getNo2() >= 100.0));

        // Finale sink to print in a CSV file
        Sink<AirQualityEvent> outputSink = anonymizationQuery.addTextFileSink("o1", "src/main/resources/resultsTestQuery.csv", true);

        anonymizationQuery.connect(inputSource, inputReader).connect(inputReader, filter1).connect(filter1, aggregateOperator).connect(aggregateOperator, filter2).connect(filter2, outputSink);

        System.out.println("*** Anonymization query activated ***");
        anonymizationQuery.activate();
    }

    private static class AggregateWindow extends BaseTimeWindowAddRemove<AirQualityEvent, AirQualityEvent> {

        private int count = 0;
        private double sumCO = 0.0;
        private double sumNO2 = 0.0;
        private AirQualityEvent lastEvent = null;
        private long lastOutputTs = -1L;

        @Override
        public void add(AirQualityEvent event) {
            if (event != null && !Double.isNaN(event.getCoLevel()) && !Double.isNaN(event.getNo2())) {
                sumCO += event.getCoLevel();
                sumNO2 += event.getNo2();
                count++;
                lastEvent = event;
            }
        }

        @Override
        public void remove(AirQualityEvent event) {
            if (event != null && !Double.isNaN(event.getCoLevel()) && !Double.isNaN(event.getNo2())) {
                sumCO -= event.getCoLevel();
                sumNO2 -= event.getNo2();
                count--;
            }
        }

        @Override
        public AirQualityEvent getAggregatedResult() {
            if (count == 0 || lastEvent == null) {
                return null;
            }

            // Avoid duplicates due to the previous filter operator in the pipeline
            // If the last event in the window is the same as the one in the last output, ignore it
            if (lastEvent.getTimestamp() == lastOutputTs) {
                return null;
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
