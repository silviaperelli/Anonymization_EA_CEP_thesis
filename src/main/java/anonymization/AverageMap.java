package anonymization;

import component.operator.in1.map.MapFunction;
import event.AirQualityEvent;
import java.util.ArrayDeque;
import java.util.Deque;

public class AverageMap implements MapFunction<AirQualityEvent, AirQualityEvent> {

    private final Deque<AirQualityEvent> windowBuffer = new ArrayDeque<>();
    private final long windowSizeMillis;
    private double currentSum = 0.0;

    public AverageMap(long windowSizeMillis) {
        this.windowSizeMillis = windowSizeMillis;
    }

    @Override
    public AirQualityEvent apply(AirQualityEvent currentEvent) {

        // Add tuple to the window
        windowBuffer.addLast(currentEvent);
        currentSum += currentEvent.getCoLevel();

        // Remove tuple to the window
        long windowStartTime = currentEvent.getTimestamp() - windowSizeMillis;
        while (!windowBuffer.isEmpty() && windowBuffer.peekFirst().getTimestamp() < windowStartTime) {
            AirQualityEvent eventToRemove = windowBuffer.removeFirst();
            currentSum -= eventToRemove.getCoLevel();
        }

        // Calculate average COLevel in the window
        int currentWindowSize = windowBuffer.size();
        double averageCoLevel = (currentWindowSize > 0) ? currentSum / currentWindowSize : 0.0;

        // Return complete event with average COLevel
        AirQualityEvent anonymizedEvent = new AirQualityEvent(currentEvent);
        anonymizedEvent.setCoLevel(averageCoLevel);
        return anonymizedEvent;
    }
}

