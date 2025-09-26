package event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;

public class AirQualityEvent {

    private static final Logger logger = LoggerFactory.getLogger(AirQualityEvent.class);

    private LocalDateTime eventTime;
    private double coLevel;

    public AirQualityEvent(LocalDateTime eventTime, double coLevel) {
        this.eventTime = eventTime;
        this.coLevel = coLevel;
    }

    public LocalDateTime getEventTime() {return eventTime;}
    public void setEventTime(LocalDateTime eventTime) {this.eventTime = eventTime;}
    public double getCoLevel() {return coLevel;}
    public void setCoLevel(double coLevel) {this.coLevel = coLevel;}

    // Formatter per il timestamp nel formato "DD/MM/YYYY HH:mm:ss"
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    // Method to create an event from a row in the CSV file
    public static AirQualityEvent eventCreation(String line) {
        try {
            String[] tokens = line.split(";", -1);
            if (tokens.length < 3) {
                return null; // Riga non valida
            }

            // Combines date and time and creates a single timestamp
            String date = tokens[0];
            String time = tokens[1].replace('.', ':');
            LocalDateTime timestamp = LocalDateTime.parse(date + " " + time, formatter);

            // Change the comma as separator for decimal number
            String coStr = tokens[2].replace(',', '.');
            double coValue = Double.parseDouble(coStr);

            // Discard missing value
            if (coValue == -200) {
                return null;
            }

            return new AirQualityEvent(timestamp, coValue);
        } catch (Exception e) {
            logger.warn("Error parsing line: '{}'", line, e);
            return null;
        }
    }

    public long getTimestamp() {
        return this.eventTime.toEpochSecond(ZoneOffset.UTC) * 1000;
    }

    @Override
    public String toString() {
        return "{" +
                "eventTime=" + eventTime +
                ", coLevel=" + coLevel +
                '}';
    }
}
