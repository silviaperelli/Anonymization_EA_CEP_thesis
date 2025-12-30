package jgea.metrics.performance.utils;

import jgea.query.MainQuery;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

// Collector for Liebre metrics
public class MetricsConsumer {

    // Map with the metric name and the couple (timestamp, metric value)
    private final Map<String, Map<Long,Long>> metricHistory = new ConcurrentHashMap<>();

    private record MetricNameSet(
            String afterSource, String beforeFilter1,
            String afterFilter1, String beforeAggregate,
            String afterAggregate, String beforeFilter2,
            String afterFilter2, String beforeSink
    ) {}

    public record TupleMetrics(long afterFilter1, long beforeAggregate, long afterAggregate, long beforeFilter2,
                               long afterFilter2, long beforeSink, long afterSource, long beforeFilter1) {}

    private MetricNameSet generateMetricNames(String queryId) {
        return new MetricNameSet(
                String.format("I1_%s_rec_as_%s.IN", queryId, queryId),
                String.format("rec_as_%s_filter1_%s.IN", queryId, queryId),
                String.format("filter1_%s_rec_af1_%s.IN", queryId, queryId),
                String.format("rec_af1_%s_average_%s.IN", queryId, queryId),
                String.format("average_%s_rec_aa_%s.IN", queryId, queryId),
                String.format("rec_aa_%s_filter2_%s.IN", queryId, queryId),
                String.format("filter2_%s_rec_af2_%s.IN", queryId, queryId),
                String.format("rec_af2_%s_o1_%s.IN", queryId, queryId)
        );
    }

    // Build and return a map of the metric
    public HashMap<String, Consumer<Object[]>> buildConsumers(String queryId) {
        // Default consumer used for any metric that Liebre generates but it's not used here
        Consumer<Object[]> doNothingConsumer = data -> {};

        // Hashmap that returns the 'doNothingConsumer' by default
        // if a requested metric key is not found. This prevents NullPointerExceptions
        HashMap<String, Consumer<Object[]>> consumers = new HashMap<>() {
            @Override
            public Consumer<Object[]> get(Object key) {
                return super.getOrDefault(key, doNothingConsumer);
            }
        };

        MetricNameSet names = generateMetricNames(queryId);
        // List of all the specific metrics
        List<String> allMetrics = List.of(
                names.afterSource(), names.beforeFilter1(),
                names.afterFilter1(), names.beforeAggregate(),
                names.afterAggregate(), names.beforeFilter2(),
                names.afterFilter2(), names.beforeSink()
        );

        // For each metric define the consumer
        for (String metricName : allMetrics) {

            consumers.put(metricName, data -> {
                long timestamp = (Long) data[0];
                long value = (Long) data[1];

                // Get the history map for the specific metric, create it if it doesn't exist
                Map<Long, Long> history = metricHistory.get(metricName);
                if (history == null) {
                    history = new ConcurrentHashMap<>();
                    metricHistory.put(metricName, history);
                }

                // Add the new (timestamp, value) pair to the map
                history.put(timestamp, value);
            });
        }
        return consumers;
    }

    // Retrieve the entire map for all metrics
    public Map<String, Map<Long, Long>> getAllMetrics() {
        return metricHistory;
    }

    // Calculate the number of valid tuples passing through a stream
    public long getCount(String metricName) {
        Map<Long, Long> history = metricHistory.get(metricName);
        if (history == null || history.isEmpty()) {
            return 0;
        }
        return history.values().stream()
                .filter(value -> value > 0)  // Skip invalid values (-1)
                .mapToLong(Long::longValue)
                .sum();
    }

    // Retrieves the average rate for each metric
    public TupleMetrics getTupleMetrics(String queryId) {
        MetricNameSet names = generateMetricNames(queryId);
        return new TupleMetrics(
                getCount(names.afterFilter1()),
                getCount(names.beforeAggregate()),
                getCount(names.afterAggregate()),
                getCount(names.beforeFilter2()),
                getCount(names.afterFilter2()),
                getCount(names.beforeSink()),
                getCount(names.afterSource()),
                getCount(names.beforeFilter1())
        );
    }

    // Retrieves the average rate for each metric
    public MainQuery.PerformanceMetrics getMetrics(String queryId) {
        MetricNameSet names = generateMetricNames(queryId);
        return new MainQuery.PerformanceMetrics(
                getCount(names.afterFilter1()),
                getCount(names.beforeAggregate()),
                getCount(names.afterAggregate()),
                getCount(names.beforeFilter2()),
                getCount(names.afterFilter2()),
                getCount(names.beforeSink()),
                getCount(names.afterSource()),
                getCount(names.beforeFilter1())
        );
    }
}