package jgea.query;

import com.codahale.metrics.Gauge;

import common.metrics.Metric;
import common.metrics.Metrics;
import common.metrics.MetricsFactory;
import common.util.Util;
import component.operator.Operator;
import component.operator.in1.aggregate.BaseTimeWindowAddRemove;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import component.operator.in1.map.MapFunction;
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
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class MainQueryKeys {

    // Record to contain the performance metrics during a query run
    public record PerformanceMetrics(
            long afterSource, long beforeFilter1, long afterFilter1,
            long beforeAggregate, long afterAggregate, long beforeFilter2,
            long afterFilter2, long beforeSink,
            long keysAfterSource, long keysAfterFilter1,
            long keysAfterAggregate, long keysOutput) {
    }

    // Record to contain the final results events and the collected performance metrics
    public record QueryResult(List<AirQualityEvent> events, PerformanceMetrics metrics) {
    }

    public static QueryResult process(List<AirQualityEvent> inputStream, String queryId) throws IOException {
        String metricsFilePath = "src/main/resources/queryMetrics";
        try {
            Files.createDirectories(Paths.get(metricsFilePath));
        } catch (IOException e) {
            throw e;
        }

        if (inputStream == null || inputStream.isEmpty()) {
            return new QueryResult(Collections.emptyList(), new PerformanceMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0));
        }

        // Create a metric collector for the run
        MetricsConsumer consumer = new MetricsConsumer();
        MetricsFactory metrics = Metrics.fileAndConsumer(metricsFilePath, consumer.buildConsumers(queryId));
        LiebreContext.mergeWithStreamMetrics(metrics);

        // Create hash map to collect unique keys
        final Set<String> keysAfterSource = ConcurrentHashMap.newKeySet();
        final Set<String> keysAfterFilter1 = ConcurrentHashMap.newKeySet();
        final Set<String> keysAfterAggregate = ConcurrentHashMap.newKeySet();
        final Set<String> finalKeys = ConcurrentHashMap.newKeySet();

        Metrics.metricRegistry().gauge("uniqueKeys_afterSource_" + queryId,
                () -> (Gauge<Integer>) () -> keysAfterSource.size());
        Metrics.metricRegistry().gauge("uniqueKeys_afterFilter1_" + queryId,
                () -> (Gauge<Integer>) () -> keysAfterFilter1.size());
        Metrics.metricRegistry().gauge("uniqueKeys_afterAggregate_" + queryId,
                () -> (Gauge<Integer>) () -> keysAfterAggregate.size());
        Metrics.metricRegistry().gauge("uniqueKeys_output_" + queryId,
                () -> (Gauge<Integer>) () -> finalKeys.size());

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

        // Operator to filter tuple with aggregate CO level >= 5.0 and aggregate NO2 level >= 100.0
        Operator<AirQualityEvent, AirQualityEvent> filter2 = query.addFilterOperator(
                "filter2_" + queryId,
                tuple -> (tuple.getCoLevel() >= 5.0 && tuple.getNo2() >= 100.0));

        // Build a hashmap of extra consumers to record unique keys at different stages
        HashMap<String, Consumer<Object[]>> keyConsumers = new HashMap<>();
        keyConsumers.put("uniqueKeys_afterSource_" + queryId + ".keys", data -> {});
        keyConsumers.put("uniqueKeys_afterFilter1_" + queryId + ".keys", data -> {});
        keyConsumers.put("uniqueKeys_afterAggregate_" + queryId + ".keys", data -> {});
        keyConsumers.put("uniqueKeys_output_" + queryId + ".keys", data -> {});

        MetricsFactory keyMetrics = Metrics.fileAndConsumer(metricsFilePath, keyConsumers);
        LiebreContext.mergeWithStreamMetrics(keyMetrics);

        class InnerMainQueryKeys implements MapFunction<AirQualityEvent, AirQualityEvent> {

            private final Set<String> keySetToPopulate;
            private final String id;
            private Metric keyMetric;

            public InnerMainQueryKeys(String id, Set<String> keySetToPopulate) {
                this.id = id;
                this.keySetToPopulate = keySetToPopulate;
                keyMetric = keyMetrics.newCountPerSecondMetric(id, "keys");
            }

            @Override
            public void enable() {
                keyMetric.enable();
            }

            @Override
            public AirQualityEvent apply(AirQualityEvent t) {
                if (t != null && t.getKey() != null) {
                    // If the key is not in the Set, it is added
                    if (keySetToPopulate.add(t.getKey())) {
                        keyMetric.record(1);
                    }
                }
                return t;
            }

            @Override
            public void disable() {
                keyMetric.disable();
            }
        }

        Operator<AirQualityEvent, AirQualityEvent> keyRecorderAfterSource = query.addMapOperator("rec_as_" + queryId,
                new InnerMainQueryKeys("uniqueKeys_afterSource_" + queryId, keysAfterSource));
        Operator<AirQualityEvent, AirQualityEvent> keyRecorderAfterFilter1 = query.addMapOperator("rec_af1_" + queryId,
                new InnerMainQueryKeys("uniqueKeys_afterFilter1_" + queryId, keysAfterFilter1));
        Operator<AirQualityEvent, AirQualityEvent> keyRecorderAfterAggregate = query.addMapOperator("rec_aa_" + queryId,
                new InnerMainQueryKeys("uniqueKeys_afterAggregate_" + queryId, keysAfterAggregate));
        Operator<AirQualityEvent, AirQualityEvent> keyRecorderAfterFilter2 = query.addMapOperator("rec_af2_" + queryId,
                new InnerMainQueryKeys("uniqueKeys_output_" + queryId, finalKeys));

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

        LiebreContext.unmergeFromStreamMetrics(metrics);
        LiebreContext.unmergeFromStreamMetrics(keyMetrics);

        // Obtain tuples counter from consumer
        MetricsConsumer.TupleMetrics tupleMetrics = consumer.getTupleMetrics(queryId);

        // Obtain keys counter dai Set popolati
        long keysAS = keysAfterSource.size();
        long keysAF1 = keysAfterFilter1.size();
        long keysAA = keysAfterAggregate.size();
        long keysO = finalKeys.size();

        // Create final record
        PerformanceMetrics finalMetrics = new PerformanceMetrics(
                tupleMetrics.afterSource(), tupleMetrics.beforeFilter1(), tupleMetrics.afterFilter1(),
                tupleMetrics.beforeAggregate(), tupleMetrics.afterAggregate(), tupleMetrics.beforeFilter2(),
                tupleMetrics.afterFilter2(), tupleMetrics.beforeSink(),
                keysAS, keysAF1, keysAA, keysO);

        return new QueryResult(collectedEvents, finalMetrics);
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