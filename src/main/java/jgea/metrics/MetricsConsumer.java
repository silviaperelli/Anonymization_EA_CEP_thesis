package jgea.metrics;

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

    private final String metricAfterSource = "I1_filter1.IN";
    private final String metricBeforeFilter1 = "I1_filter1.OUT";
    private final String metricAfterFilter1 = "filter1_average.IN";
    private final String metricBeforeAggregate = "filter1_average.OUT";
    private final String metricAfterAggregate = "average_filter2.IN";
    private final String metricBeforeFilter2 = "average_filter2.OUT";
    private final String metricAfterFilter2 = "filter2_o1.IN";
    private final String metricBeforeSink = "filter2_o1.OUT";

    // Build and return a map of the metric
    public HashMap<String, Consumer<Object[]>> buildConsumers(String queryid) {
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

        // List of all the specific metrics
        List<String> allMetrics = List.of(
                metricAfterSource, metricBeforeFilter1,
                metricAfterFilter1, metricBeforeAggregate,
                metricAfterAggregate, metricBeforeFilter2,
                metricAfterFilter2, metricBeforeSink
        );

        // For each metric define the consumer
        for (String metricName : allMetrics) {

            consumers.put(metricName, data -> {
                long timestamp = (Long) data[0];
                long value = (Long) data[1];

                // DEBUG
                System.out.printf("[METRIC_DEBUG] Query ID: %s, Time: %d, Metric: %s, Value: %d%n",
                        queryid, timestamp, metricName, value);

                // Get the history map for the specific metric, create it if it doesn't exist
                Map<Long, Long> history = metricHistory.computeIfAbsent(
                        metricName,
                        k -> new ConcurrentHashMap<>()
                );

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

    // Calculate and return the average rate (tuples/sec) for a given metric
    public double getAverageRate(String metricName) {
        Map<Long, Long> history = metricHistory.get(metricName);
        if (history == null || history.isEmpty()) {
            return 0.0;
        }

        // Filter out invalid values (-1)
        List<Long> validCounts = history.values().stream()
                .filter(value -> value > 0)
                .toList();

        if (validCounts.isEmpty()) {
            return 0.0;
        }

        double sumOfCounts = validCounts.stream()
                .mapToDouble(Long::doubleValue)
                .sum();

        // Calculate the average
        double averageRate = sumOfCounts / validCounts.size();

        return averageRate;
    }

    // Retrieves the average rate for each metric
    public MainQuery.PerformanceMetrics getMetrics() {
        return new MainQuery.PerformanceMetrics(
                getAverageRate(metricAfterFilter1),
                getAverageRate(metricBeforeAggregate),
                getAverageRate(metricAfterAggregate),
                getAverageRate(metricBeforeFilter2),
                getAverageRate(metricAfterFilter2),
                getAverageRate(metricBeforeSink),
                getAverageRate(metricAfterSource),
                getAverageRate(metricBeforeFilter1)
        );
    }
}