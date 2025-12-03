package event;

import common.tuple.BaseRichTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Writer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class AirQualityEvent extends BaseRichTuple {

    private static final Logger logger = LoggerFactory.getLogger(AirQualityEvent.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public enum EventType {
        NORMAL,
        DUPLICATE,
        EMPTY_WINDOW
    }

    private long tupleId;
    private long sensorId;
    private LocalDateTime eventTime;
    private double coLevel; // CO(GT)
    private double pt08s1; // PT08.S1(CO)
    private double nmhc;   // NMHC(GT)
    private double c6h6;  // C6H6(GT)
    private double pt08s2; // PT08.S2(NMHC)
    private double nox;    // NOx(GT)
    private double pt08s3; // PT08.S3(NOx)
    private double no2;    // NO2(GT)
    private double pt08s4; // PT08.S4(NO2)
    private double pt08s5; // PT08.S5(O3)
    private double t;     // T
    private double rh;    // RH
    private double ah;    // AH
    private EventType eventType;

    public AirQualityEvent(long tupleId, long sensorId, LocalDateTime eventTime, double coLevel, double pt08s1, double nmhc, double c6h6, double pt08s2, double nox, double pt08s3, double no2, double pt08s4, double pt08s5, double t, double rh, double ah, EventType eventType) {
        super(eventTime.toEpochSecond(ZoneOffset.UTC) * 1000, String.valueOf(sensorId));
        this.tupleId = tupleId;
        this.sensorId = sensorId;
        this.eventTime = eventTime;
        this.coLevel = coLevel;
        this.pt08s1 = pt08s1;
        this.nmhc = nmhc;
        this.c6h6 = c6h6;
        this.pt08s2 = pt08s2;
        this.nox = nox;
        this.pt08s3 = pt08s3;
        this.no2 = no2;
        this.pt08s4 = pt08s4;
        this.pt08s5 = pt08s5;
        this.t = t;
        this.rh = rh;
        this.ah = ah;
        this.eventType = eventType;
    }

    // Copying constructor
    public AirQualityEvent(AirQualityEvent other) {
        super(other.timestamp, other.key);
        this.tupleId = other.tupleId;
        this.sensorId = other.sensorId;
        this.eventTime = other.eventTime;
        this.coLevel = other.coLevel;
        this.pt08s1 = other.pt08s1;
        this.nmhc = other.nmhc;
        this.c6h6 = other.c6h6;
        this.pt08s2 = other.pt08s2;
        this.nox = other.nox;
        this.pt08s3 = other.pt08s3;
        this.no2 = other.no2;
        this.pt08s4 = other.pt08s4;
        this.pt08s5 = other.pt08s5;
        this.t = other.t;
        this.rh = other.rh;
        this.ah = other.ah;
        this.eventType = other.eventType;
    }

    // Constructor for the output tuple of the Duplicate Map
    public AirQualityEvent(AirQualityEvent other, EventType eventType) {
        this(other);
        this.setEventType(eventType);
    }

    // Constructor for the output tuple of the Aggregate Window
    public AirQualityEvent(AirQualityEvent other, double newCoLevel, double newNoLevel) {
        this(other);
        this.setCoLevel(newCoLevel);
        this.setNo2(newNoLevel);
    }

    // Factory for empty event
    public static AirQualityEvent createEmptyEvent(long timestampMillis) {
        LocalDateTime timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestampMillis), ZoneOffset.UTC);
        return new AirQualityEvent(
                -1L, -1L, timestamp, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
                Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN,
                Double.NaN, Double.NaN, EventType.EMPTY_WINDOW
        );
    }


    public long getTupleId() {return tupleId;}
    public long getTimestamp() { return timestamp;}

    public long getSensorId() {return sensorId;}
    public void setSensorId(long sensorId) {this.sensorId = sensorId;}

    public LocalDateTime getEventTime() {return eventTime;}
    public void setEventTime(LocalDateTime eventTime) {this.eventTime = eventTime;}

    public double getCoLevel() {return coLevel;}
    public void setCoLevel(double coLevel) {this.coLevel = coLevel;}

    public double getNo2() {return no2;}
    public void setNo2(double no2) {this.no2 = no2;}

    public double getAh() {return ah;}
    public void setAh(double ah) {this.ah = ah;}

    public double getRh() {return rh;}
    public void setRh(double rh) {this.rh = rh;}

    public double getT() {return t;}
    public void setT(double t) {this.t = t;}

    public double getPt08s5() {return pt08s5;}
    public void setPt08s5(double pt08s5) {this.pt08s5 = pt08s5;}

    public double getPt08s4() {return pt08s4;}
    public void setPt08s4(double pt08s4) {this.pt08s4 = pt08s4;}

    public double getPt08s3() {return pt08s3;}
    public void setPt08s3(double pt08s3) {this.pt08s3 = pt08s3;}

    public double getNox() {return nox;}
    public void setNox(double nox) {this.nox = nox;}

    public double getPt08s2() {return pt08s2;}
    public void setPt08s2(double pt08s2) {this.pt08s2 = pt08s2;}

    public double getC6h6() {return c6h6;}
    public void setC6h6(double c6h6) {this.c6h6 = c6h6;}

    public double getNmhc() {return nmhc;}
    public void setNmhc(double nmhc) {this.nmhc = nmhc;}

    public double getPt08s1() {return pt08s1;}
    public void setPt08s1(double pt08s1) {this.pt08s1 = pt08s1;}

    public AirQualityEvent.EventType getEventType() {return eventType;}
    public void setEventType(AirQualityEvent.EventType eventType) {this.eventType = eventType;}

    // Create an event from a line in the CSV file
    public static AirQualityEvent eventCreation(String line) {
        try {
            String[] tokens = line.split(";", -1);
            if (tokens.length < 17) {
                return null;
            }

            // Parse tuple ID and sensor ID
            long id = Long.parseLong(tokens[0].trim());
            long sensorId = Long.parseLong(tokens[1].trim());

            // Combine date and time and create a single timestamp
            String date = tokens[2];
            String time = tokens[3].replace('.', ':');
            LocalDateTime timestamp = LocalDateTime.parse(date + " " + time, formatter);

            // Parse all the features
            double coValue = Writer.formatDouble(tokens[4]);
            double pt08s1Val = Writer.formatDouble(tokens[5]);
            double nmhcVal = Writer.formatDouble(tokens[6]);
            double c6h6Val = Writer.formatDouble(tokens[7]);
            double pt08s2Val = Writer.formatDouble(tokens[8]);
            double noxVal = Writer.formatDouble(tokens[9]);
            double pt08s3Val = Writer.formatDouble(tokens[10]);
            double no2Val = Writer.formatDouble(tokens[11]);
            double pt08s4Val = Writer.formatDouble(tokens[12]);
            double pt08s5Val = Writer.formatDouble(tokens[13]);
            double tVal = Writer.formatDouble(tokens[14]);
            double rhVal = Writer.formatDouble(tokens[15]);
            double ahVal = Writer.formatDouble(tokens[16]);

            // EventType setting that work with both the original and modified datasets
            EventType type = EventType.NORMAL;

            // If the event type column exists and has content, parse the string into an Enum constant
            if (tokens.length > 17 && tokens[17] != null && !tokens[17].isEmpty()) {
                try {
                    type = EventType.valueOf(tokens[17].trim().toUpperCase());
                } catch (IllegalArgumentException e) {
                    logger.warn("Unknown EventType '{}', defaulting to NORMAL", tokens[17]);
                }
            }

            return new AirQualityEvent(id, sensorId, timestamp, coValue, pt08s1Val, nmhcVal, c6h6Val, pt08s2Val, noxVal, pt08s3Val, no2Val, pt08s4Val, pt08s5Val, tVal, rhVal, ahVal, type);

        } catch (Exception e) {
            logger.warn("Error parsing line: '{}'", line, e);
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format(Locale.US,
                "%d;%d;%s;%s;%.6f;%.0f;%.0f;%.1f;%.0f;%.0f;%.0f;%.6f;%.0f;%.0f;%.1f;%.1f;%.4f;%s",
                tupleId,
                sensorId,
                eventTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")),
                eventTime.format(DateTimeFormatter.ofPattern("HH.mm.ss")),
                coLevel, pt08s1, nmhc, c6h6, pt08s2, nox, pt08s3, no2, pt08s4, pt08s5, t, rh, ah, eventType
        );
    }
}
