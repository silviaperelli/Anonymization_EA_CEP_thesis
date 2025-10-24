package jgea.query;

import common.metrics.Metrics;
import common.util.Util;
import component.operator.Operator;
import component.operator.in1.aggregate.BaseTimeWindowAddRemove;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import component.sink.Sink;
import component.source.Source;
import component.source.SourceFunction;

import event.AirQualityEvent;
import jgea.metrics.MetricsConsumer;
import query.LiebreContext;
import query.Query;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MainQuery {

    // Record to contain the performance metrics during a query run
    public record PerformanceMetrics(long afterFilter1, long beforeAggregate, long afterAggregate, long beforeFilter2,
            long afterFilter2, long beforeSink, long afterSource, long beforeFilter1) {
    }

    // Record to contain the final results events and the collected performance
    // metrics
    public record QueryResult(List<AirQualityEvent> events, PerformanceMetrics metrics) {
    }

    public static QueryResult process(List<AirQualityEvent> inputStream, String queryId) throws IOException {

        String metricsFilePath = "src/main/resources/query" + queryId;

        try {
            Files.createDirectories(Paths.get(metricsFilePath));
        } catch (IOException e) {
            System.err.println("Error while creating the metrics directories: " + metricsFilePath);
            throw e;
        }

        if (inputStream == null || inputStream.isEmpty()) {
            return new QueryResult(Collections.emptyList(), new PerformanceMetrics(0, 0, 0, 0, 0, 0, 0, 0));
        }

        // Create a metric collector for the run
        MetricsConsumer consumer = new MetricsConsumer();
        LiebreContext.setStreamMetrics(Metrics.fileAndConsumer(metricsFilePath, consumer.buildConsumers(queryId)));

        final List<AirQualityEvent> collectedEvents = Collections.synchronizedList(new ArrayList<>());
        Query query = new Query();

        // Create and add a source that reads from the provided in-memory list
        SourceFunction<AirQualityEvent> collectionSource = createCollectionSource(inputStream);
        Source<AirQualityEvent> inputSource = query.addBaseSource("I1_"+queryId, collectionSource);

        // Operator to filter tuple with CO level >= 2.0 and NO2 level >= 40.0
        Operator<AirQualityEvent, AirQualityEvent> filter1 = query.addFilterOperator(
                "filter1_" + queryId, tuple -> tuple != null && (tuple.getCoLevel() >= 2.0 && tuple.getNo2() >= 40.0));

        // Window of 3 hours, sliding every 1 hour
        final long WINDOW_SIZE = 3 * 60 * 60 * 1000;
        final long WINDOW_SLIDE = 60 * 60 * 1000;

        // Operator to aggregate the CO level and NO2 level in a window of 2 hours
        Operator<AirQualityEvent, AirQualityEvent> aggregateOperator = query.addTimeAggregateOperator("average_"+queryId,
                WINDOW_SIZE, WINDOW_SLIDE, new AggregateWindow());

        // Operator to filter tuple with aggregate CO level >= 5.0 and aggregate NO2
        // level >= 100.0
        Operator<AirQualityEvent, AirQualityEvent> filter2 = query.addFilterOperator(
                "filter2_"+queryId,
                tuple -> tuple != null && !tuple.isEmpty() && (tuple.getCoLevel() >= 5.0 && tuple.getNo2() >= 100.0));

        // Final Sink that adds every event to a list
        Sink<AirQualityEvent> sink = query.addBaseSink("o1_"+queryId, event -> {
            if (event != null) {
                collectedEvents.add(event);
            }
        });

        query.connect(inputSource, filter1)
                .connect(filter1, aggregateOperator)
                .connect(aggregateOperator, filter2)
                .connect(filter2, sink);

        query.activate();
        //Util.sleep(2000);
        //query.deActivate();


        int waitCycles = 0;

        while(sink.isEnabled()) {
            try {
                System.out.printf("[DEBUG MainQuery]    -> Ciclo di attesa %d: sink.isEnabled() è VERO. Attendo 1 secondo...%n", waitCycles + 1);
                Thread.sleep(1000);
                waitCycles++;
            } catch (InterruptedException e) {
                System.err.println("[DEBUG MainQuery] Ciclo di attesa interrotto!");
                e.printStackTrace();
            }
        }

        return new QueryResult(collectedEvents, consumer.getMetrics(queryId));

    }

    // Helper method to create a Source Function that reads from a list
    private static <T> SourceFunction<T> createCollectionSource(final List<T> list) {

        return new SourceFunction<T>() {
            private int currentIndex = 0;
            private boolean isFinished = false;
            private static final long IDLE_SLEEP = 1000;
            private boolean enabled;

            @Override
            public T get() {
                if (isFinished) {
                    Util.sleep(IDLE_SLEEP);
                    return null;
                }
                if (currentIndex < list.size()) {
                    T item = list.get(currentIndex);
                    currentIndex++;
                    return item;
                } else {
                    isFinished = true;
                    return null;
                }
            }

            @Override
            public boolean isInputFinished() {
                return isFinished;
            }

            @Override
            public void enable() {
                this.enabled = true;
            }

            @Override
            public boolean isEnabled() {
                return enabled;
            }

            @Override
            public void disable() {
                this.enabled = false;
            }

            @Override
            public boolean canRun() {
                return !isFinished;
            }

        };
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
