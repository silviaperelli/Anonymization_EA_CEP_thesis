package anonymization;

import component.operator.in1.map.MapFunction;
import event.AirQualityEvent;
import java.util.Random;

public class NoiseOperator implements MapFunction<AirQualityEvent, AirQualityEvent> {

    private final Random random;
    private final double stdDev; // Deviazione standard: controlla l'intensità del rumore

    public NoiseOperator(double stdDev) {
        this.random = new Random();
        this.stdDev = stdDev;
    }

    @Override
    public AirQualityEvent apply(AirQualityEvent airQualityEvent) {
        if (airQualityEvent == null) {
            return null;
        }

        // Genera un valore da una distribuzione Normale con media 0 e deviazione standard 'stdDev'
        double noise = random.nextGaussian() * stdDev;

        double originalCoLevel = airQualityEvent.getCoLevel();
        double noisyCoLevel = originalCoLevel + noise;

        // Assicurati che il valore non diventi negativo (improbabile ma possibile)
        noisyCoLevel = Math.max(0, noisyCoLevel);

        // Clona l'evento e modifica solo il coLevel. Devi implementare un metodo per clonare
        // o un costruttore di copia nella tua classe AirQualityEvent.
        AirQualityEvent noisyEvent = new AirQualityEvent(airQualityEvent); // Ipotizzando un costruttore di copia
        noisyEvent.setCoLevel(noisyCoLevel);

        return noisyEvent;
    }
}
