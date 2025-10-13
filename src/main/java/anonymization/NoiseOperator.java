package anonymization;

import component.operator.in1.map.MapFunction;
import event.AirQualityEvent;
import java.util.Random;

public class NoiseOperator implements MapFunction<AirQualityEvent, AirQualityEvent> {

    private final Random random;
    private final double stdDev; // Standard deviation for noise intensity

    public NoiseOperator(double stdDev) {
        this.random = new Random();
        this.stdDev = stdDev;
    }

    @Override
    public AirQualityEvent apply(AirQualityEvent airQualityEvent) {
        if (airQualityEvent == null) {
            return null;
        }

        // Generate a value from a Normal distribution with mean 0 and standard deviation 'stdDev'
        double noise = random.nextGaussian() * stdDev;

        double originalCoLevel = airQualityEvent.getCoLevel();
        // Add the noise to the original value
        double noisyCoLevel = originalCoLevel + noise;
        noisyCoLevel = Math.max(0, noisyCoLevel);

        AirQualityEvent noisyEvent = new AirQualityEvent(airQualityEvent);
        noisyEvent.setCoLevel(noisyCoLevel);

        return noisyEvent;
    }
}
