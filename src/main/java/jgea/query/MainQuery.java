package jgea.query;

import common.util.Util;
import component.operator.Operator;
import component.operator.in1.aggregate.BaseTimeWindowAddRemove;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import component.sink.Sink;
import component.source.Source;
import component.source.SourceFunction;

import event.AirQualityEvent;
import query.Query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class MainQuery {

    // TO DO: add metrics calculation

    public static List<AirQualityEvent> process(List<AirQualityEvent> inputStream) throws Exception{

        Query query = new Query();
        final List<AirQualityEvent> collectedEvents = Collections.synchronizedList(new ArrayList<>());
        // Create and add a source that reads from the provided in-memory list
        SourceFunction<AirQualityEvent> collectionSource = createCollectionSource(inputStream);
        Source<AirQualityEvent> inputSource = query.addBaseSource("I1", collectionSource);

        // Operator to filter tuple with CO level >= 2.0 and NO2 level >= 40.0
        Operator<AirQualityEvent, AirQualityEvent> filter1 = query.addFilterOperator(
                "filter1", tuple -> tuple != null && (tuple.getCoLevel() >= 2.0 && tuple.getNo2() >= 40.0));

        // Window of 3 hours, sliding every 1 hour
        final long WINDOW_SIZE = 3 * 60 * 60 * 1000;
        final long WINDOW_SLIDE = 60 * 60 * 1000;

        // Operator to aggregate the CO level and NO2 level in a window of 2 hours
        Operator<AirQualityEvent, AirQualityEvent> aggregateOperator =
                query.addTimeAggregateOperator("average", WINDOW_SIZE, WINDOW_SLIDE, new AggregateWindow());

        // Operator to filter tuple with aggregate CO level >= 5.0 and aggregate NO2 level >= 100.0
        Operator<AirQualityEvent, AirQualityEvent> filter2 = query.addFilterOperator(
                "filter2", tuple -> tuple != null && !tuple.isEmpty() && (tuple.getCoLevel() >= 5.0 && tuple.getNo2() >= 100.0));

        // Final Sink that adds every event to a list
        Sink<AirQualityEvent> sink = query.addBaseSink("output-sink", event -> {
            if (event != null) {
                collectedEvents.add(event);
            }
        });

        query.connect(inputSource, filter1)
                .connect(filter1, aggregateOperator)
                .connect(aggregateOperator, filter2)
                .connect(filter2, sink);

        query.activate();

        Util.sleep(1000);
        query.deActivate();

        return collectedEvents;

    }

    // Helper method to create a Source Function that reads from a list
    private static <T> SourceFunction<T> createCollectionSource(final List<T> list) {

        return new SourceFunction<T>() {
            private int currentIndex = 0;
            private boolean isFinished = false;

            @Override
            public T get() {
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
            // If the last event in the window is the same as the one in the last output, ignore it
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
