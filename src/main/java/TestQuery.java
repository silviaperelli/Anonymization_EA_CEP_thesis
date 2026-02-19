import component.operator.Operator;
import component.sink.Sink;
import component.source.Source;
import event.DataLoader;
import event.EventFactory;
import event.GenericEvent;
import query.Query;
import jgea.query.utils.MovingAggregateMap;
import jgea.mappers.QueryRepresentation;

import java.io.IOException;
import java.util.*;

// TestQuery for the pipeline: Pipeline { map_duplicate(probability=0.30) | map_aggregate(attribute=avg_Y, function=avg, window=7) |
// filter(avg_X > 5.0200) | map_aggregate(attribute=avg_Y, function=min, window=6) | filter(avg_Y > 0.0000) }

public class TestQuery {

    public static void main(String[] args) throws IOException, InterruptedException {

        final String inputFile = "src/main/resources/datasets/geolife_60mins.csv";
        final String inputCsvPath = "datasets/geolife_60mins.csv";
        final String keyColumn = "user";

        DataLoader loader = new DataLoader(inputCsvPath, keyColumn);
        DataLoader.LoadResult result = loader.load();
        EventFactory.setNumericAttributes(new HashSet<>(result.numericAttributes()));

        String[] headers = new String[]{"timestamp", "user", "avg_X", "avg_Y"};

        Query query = new Query();
        Source<String> inputSource = query.addTextFileSource("I1", inputFile);

        long[] idCounter = {0};

        Operator<String, GenericEvent> reader = query.addMapOperator(
                "reader",
                line -> {
                    if (line.startsWith("timestamp,user")) return null;
                    return EventFactory.createEventFromLine(line, headers, keyColumn, idCounter[0]++);
                });

        Random random = new Random();
        Operator<GenericEvent, GenericEvent> duplicateOperator =
                query.addFlatMapOperator(
                        "map_duplicate",
                        event -> {
                            List<GenericEvent> results = new ArrayList<>();
                            results.add(event);

                            if (random.nextDouble() < 0.30) {
                                results.add(new GenericEvent(event, GenericEvent.EventType.DUPLICATE));
                            }

                            return results;
                        });

        Operator<GenericEvent, GenericEvent> aggregateAvgY =
                query.addMapOperator(
                        "map_aggregate_avgY",
                        new MovingAggregateMap(
                                "avg_Y",
                                QueryRepresentation.AggregationFunction.AVG,
                                7
                        )
                );

        Operator<GenericEvent, GenericEvent> filterX =
                query.addFilterOperator(
                        "filter_avgX",
                        event -> {
                            if (event == null) return false;
                            double x = event.getAttribute("avg_X");
                            return !Double.isNaN(x) && x > 5.0200;
                        });

        Operator<GenericEvent, GenericEvent> aggregateMinY =
                query.addMapOperator(
                        "map_aggregate_minY",
                        new MovingAggregateMap(
                                "avg_Y",
                                QueryRepresentation.AggregationFunction.MIN,
                                6
                        )
                );

        Operator<GenericEvent, GenericEvent> filterY =
                query.addFilterOperator(
                        "filter_avgY",
                        event -> {
                            if (event == null) return false;
                            double y = event.getAttribute("avg_Y");
                            return !Double.isNaN(y) && y > 0.0;
                        });

        Operator<GenericEvent, String> formatter =
                query.addMapOperator("formatter", event -> {
                    if (event == null) return null;

                    return event.getTimestamp() + "," +
                            event.getKey() + "," +
                            event.getAttribute("avg_X") + "," +
                            event.getAttribute("avg_Y");
                });

        Sink<String> outputSink =
                query.addTextFileSink("o1", "results_pipeline_test.csv", true);

        query.connect(inputSource, reader)
                .connect(reader, duplicateOperator)
                .connect(duplicateOperator, aggregateAvgY)
                .connect(aggregateAvgY, filterX)
                .connect(filterX, aggregateMinY)
                .connect(aggregateMinY, filterY)
                .connect(filterY, formatter)
                .connect(formatter, outputSink);

        query.activate();

        System.out.println("*** Query activated ***");

        while (outputSink.isEnabled()) {
            Thread.sleep(10);
        }

        query.deActivate();
        System.out.println("*** Query completed ***");
    }
}
