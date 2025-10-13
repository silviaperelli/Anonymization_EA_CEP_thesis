package event;

import common.tuple.BaseRichTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.util.Locale;
import utils.Writer;

public class AirQualityEvent extends BaseRichTuple{

    private static final Logger logger = LoggerFactory.getLogger(AirQualityEvent.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

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

    public AirQualityEvent(LocalDateTime eventTime, double coLevel, double pt08s1, double nmhc, double c6h6, double pt08s2, double nox, double pt08s3, double no2, double pt08s4, double pt08s5, double t, double rh, double ah) {
        super(eventTime.toEpochSecond(ZoneOffset.UTC) * 1000, "1");
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
    }

    // Add a copying constructor
    public AirQualityEvent(AirQualityEvent other) {
        super(other.timestamp, other.key);
        this.coLevel = other.coLevel;
        this.eventTime = other.eventTime;
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
    }

    // Add a constructor for the output tuple of the Average Window
    public AirQualityEvent(AirQualityEvent other, double newCoLevel) {
        this(other);
        this.setCoLevel(newCoLevel);
    }

    public LocalDateTime getEventTime() {return eventTime;}
    public void setEventTime(LocalDateTime eventTime) {this.eventTime = eventTime;}

    public double getCoLevel() {return coLevel;}
    public void setCoLevel(double coLevel) {this.coLevel = coLevel;}

    public long getTimestamp() { return timestamp;}

    // Create an event from a line in the CSV file
    public static AirQualityEvent eventCreation(String line) {
        try {
            String[] tokens = line.split(";", -1);
            if (tokens.length < 15) {
                return null;
            }

            // Combine date and time and create a single timestamp
            String date = tokens[0];
            String time = tokens[1].replace('.', ':');
            LocalDateTime timestamp = LocalDateTime.parse(date + " " + time, formatter);

            // Parse all the features
            double coValue = Writer.formatDouble(tokens[2]);
            double pt08s1Val = Writer.formatDouble(tokens[3]);
            double nmhcVal = Writer.formatDouble(tokens[4]);
            double c6h6Val = Writer.formatDouble(tokens[5]);
            double pt08s2Val = Writer.formatDouble(tokens[6]);
            double noxVal = Writer.formatDouble(tokens[7]);
            double pt08s3Val = Writer.formatDouble(tokens[8]);
            double no2Val = Writer.formatDouble(tokens[9]);
            double pt08s4Val = Writer.formatDouble(tokens[10]);
            double pt08s5Val = Writer.formatDouble(tokens[11]);
            double tVal = Writer.formatDouble(tokens[12]);
            double rhVal = Writer.formatDouble(tokens[13]);
            double ahVal = Writer.formatDouble(tokens[14]);

            return new AirQualityEvent(timestamp, coValue, pt08s1Val, nmhcVal, c6h6Val, pt08s2Val, noxVal, pt08s3Val, no2Val, pt08s4Val, pt08s5Val, tVal, rhVal, ahVal);

        } catch (Exception e) {
            logger.warn("Error parsing line: '{}'", line, e);
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format(Locale.US,
                "%s;%s;%.6f;%.0f;%.0f;%.1f;%.0f;%.0f;%.0f;%.0f;%.0f;%.0f;%.1f;%.1f;%.4f",
                eventTime.format(DateTimeFormatter.ofPattern("dd/MM/yyyy")),
                eventTime.format(DateTimeFormatter.ofPattern("HH.mm.ss")),
                coLevel, pt08s1, nmhc, c6h6, pt08s2, nox, pt08s3, no2, pt08s4, pt08s5, t, rh, ah
        );
    }

    public double getAh() {return ah;}

    public double getRh() {return rh;}

    public double getT() {return t;}

    public double getPt08s5() {return pt08s5;}

    public double getPt08s4() {return pt08s4;}

    public double getNo2() {return no2;}

    public double getPt08s3() {return pt08s3;}

    public double getNox() {return nox;}

    public double getPt08s2() {return pt08s2;}

    public double getC6h6() {return c6h6;}

    public double getNmhc() {return nmhc;}

    public double getPt08s1() {return pt08s1;}

}