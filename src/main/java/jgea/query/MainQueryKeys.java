package jgea.query;

import common.util.Util;
import component.operator.Operator;
import component.operator.in1.aggregate.BaseTimeWindowAddRemove;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import component.operator.in1.map.MapFunction;
import component.sink.Sink;
import component.source.Source;
import component.source.SourceFunction;
import event.AirQualityEvent;
import query.Query;
import jgea.metrics.performance.utils.StreamStatsWindow;

import java.io.IOException;
import java.util.*;

public class MainQueryKeys {

    // Record to contain the final results events and the collected performance metrics
    public record QueryResult(List<AirQualityEvent> events, StreamStatsWindow statsWindow) {}

    public static QueryResult process(List<AirQualityEvent> inputStream, String queryId) throws IOException {

        long minTs = 1078941600000L; // 2004-03-10 18:00:00 UTC
        long maxTs = 1112623200000L; // 2005-04-04 14:00:00 UTC
        long resolution = 3600000L;  // 1 Ora in millisecondi

        StreamStatsWindow statsWindow = new StreamStatsWindow(
                Set.of("sourceStream", "afterFilter1", "afterAggregate", "outputStream"),
                minTs, maxTs, resolution);

        if (inputStream == null || inputStream.isEmpty()) {
            // Create an empty StreamStatsWindow
            StreamStatsWindow emptyStats = new StreamStatsWindow(
                    Set.of("sourceStream", "afterFilter1", "afterAggregate", "outputStream"),
                    minTs, maxTs, resolution);
            return new QueryResult(Collections.emptyList(), emptyStats);
        }

        final List<AirQualityEvent> collectedEvents = Collections.synchronizedList(new ArrayList<>());
        Query query = new Query();

        // Create and add a source that reads from the provided in-memory list
        SourceFunction<AirQualityEvent> collectionSource = createCollectionSource(inputStream);
        Source<AirQualityEvent> inputSource = query.addBaseSource("I1_" + queryId, collectionSource);

        // Operator to filter tuple with CO level >= 2.0 and NO2 level >= 40.0
        Operator<AirQualityEvent, AirQualityEvent> filter1 = query.addFilterOperator(
                "filter1_" + queryId, tuple -> (tuple.getCoLevel() >= 2.0 && tuple.getNo2() >= 40.0));

        // Window of 3 hours, sliding every 1 hour
        final long WINDOW_SIZE = 3 * 60 * 60 * 1000;
        final long WINDOW_SLIDE = 60 * 60 * 1000;

        // Operator to aggregate the CO level and NO2 level in a window of 3 hours
        Operator<AirQualityEvent, AirQualityEvent> aggregateOperator = query.addTimeAggregateOperator(
                "average_" + queryId,
                WINDOW_SIZE, WINDOW_SLIDE, new AggregateWindow());

        // Operator to filter tuple with aggregate CO level >= 5.0 and aggregate NO2
        // level >= 100.0
        Operator<AirQualityEvent, AirQualityEvent> filter2 = query.addFilterOperator(
                "filter2_" + queryId,
                tuple -> (tuple.getCoLevel() >= 5.0 && tuple.getNo2() >= 100.0));

        class InnerMainQueryKeys implements MapFunction<AirQualityEvent, AirQualityEvent> {

            private final HashSet<String> keysSet = new HashSet<>();
            private final String id;
            private final StreamStatsWindow statsWindowLocal;

            private long currentPerformanceMetricTimestamp = -1L;

            public InnerMainQueryKeys(String id, StreamStatsWindow statsWindowLocal) {
                this.id = id;
                this.statsWindowLocal = statsWindowLocal;
            }

            @Override
            public AirQualityEvent apply(AirQualityEvent t) {
                if (t != null) {

                    // Calculate the bucket index for the current event's timestamp.
                    long bucketIndex =
                            (t.getTimestamp() - statsWindowLocal.minTimestamp())
                                    / statsWindowLocal.getResolutionMillis();

                    if (currentPerformanceMetricTimestamp != -1
                            && currentPerformanceMetricTimestamp != bucketIndex) {
                        // New timestamp, reset the keys set
                        keysSet.clear();
                    }
                    currentPerformanceMetricTimestamp = bucketIndex;

                    // Reconstruct the aligned timestamp for the start of the current bucket to use in the method addKeys and addTuples
                    long alignedTs = statsWindowLocal.minTimestamp() + bucketIndex * statsWindowLocal.getResolutionMillis();

                    // Clamp timestamp to avoid out-of-bounds generated from the aggregation
                    if (alignedTs < statsWindowLocal.minTimestamp()) {
                        alignedTs = statsWindowLocal.minTimestamp();
                    }
                    if (alignedTs > statsWindowLocal.maxTimestamp()) {
                        alignedTs = statsWindowLocal.maxTimestamp();
                    }

                    // Update the performance statistics
                    if (!keysSet.contains(t.getKey())
                            && t.getEventType() != AirQualityEvent.EventType.EMPTY_WINDOW) {
                        keysSet.add(t.getKey());
                        statsWindowLocal.addKeys(id, alignedTs, 1);
                    }

                    statsWindowLocal.addTuples(id, alignedTs, 1);
                }

                return t;
            }
        }

        Operator<AirQualityEvent, AirQualityEvent> keyRecorderAfterSource = query.addMapOperator("rec_as_" + queryId,
                new InnerMainQueryKeys("sourceStream", statsWindow));
        Operator<AirQualityEvent, AirQualityEvent> keyRecorderAfterFilter1 = query.addMapOperator("rec_af1_" + queryId,
                new InnerMainQueryKeys("afterFilter1", statsWindow));
        Operator<AirQualityEvent, AirQualityEvent> keyRecorderAfterAggregate = query.addMapOperator("rec_aa_" + queryId,
                new InnerMainQueryKeys("afterAggregate", statsWindow));
        Operator<AirQualityEvent, AirQualityEvent> keyRecorderAfterFilter2 = query.addMapOperator("rec_af2_" + queryId,
                new InnerMainQueryKeys("outputStream", statsWindow));

        // Final Sink that adds every event to a list
        Sink<AirQualityEvent> sink = query.addBaseSink("o1_" + queryId, event -> {
            if (event != null && event.getEventType() != AirQualityEvent.EventType.EMPTY_WINDOW) {
                collectedEvents.add(event);
            }
        });

        query.connect(inputSource, keyRecorderAfterSource)
                .connect(keyRecorderAfterSource, filter1)
                .connect(filter1, keyRecorderAfterFilter1)
                .connect(keyRecorderAfterFilter1, aggregateOperator)
                .connect(aggregateOperator, keyRecorderAfterAggregate)
                .connect(keyRecorderAfterAggregate, filter2)
                .connect(filter2, keyRecorderAfterFilter2)
                .connect(keyRecorderAfterFilter2, sink);

        query.activate();

        while (sink.isEnabled()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        query.deActivate();

        return new QueryResult(collectedEvents, statsWindow);
    }

    // Helper method to create a Source Function that reads from a list
    private static <T> SourceFunction<T> createCollectionSource(final List<T> list) {

        return new SourceFunction<T>() {
            private int currentIndex = 0;
            private boolean isFinished = false;
            private static final long IDLE_SLEEP = 10;
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