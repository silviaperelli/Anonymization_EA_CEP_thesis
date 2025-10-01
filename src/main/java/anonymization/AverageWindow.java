package anonymization;

import component.operator.in1.aggregate.BaseTimeWindowAddRemove;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import event.AirQualityEvent;

public class AverageWindow extends BaseTimeWindowAddRemove <AirQualityEvent, AirQualityEvent>{

    private int count = 0;
    private double sum = 0.0;
    private AirQualityEvent lastEvent; //keeps track of the last event in the window
    private long lastOutputTs = -1;

    @Override
    public void add(AirQualityEvent event) {
        sum += event.getCoLevel();
        count++;
        lastEvent = event;
    }

    @Override
    public void remove(AirQualityEvent event) {
        sum -= event.getCoLevel();
        count--;
    }

    @Override
    public AirQualityEvent getAggregatedResult() {
        if (count == 0 || lastEvent == null) {
            return null;
        }

        // Avoid duplicates due to the previous filter operatore in the pipeline
        if (lastEvent.getTimestamp() == lastOutputTs) {
            return null;
        }
        lastOutputTs = lastEvent.getTimestamp();

        double average = sum / count;
        return new AirQualityEvent(lastEvent, average);
    }

    @Override
    public TimeWindowAddRemove<AirQualityEvent, AirQualityEvent> factory(){
        return new AverageWindow();
    }
}