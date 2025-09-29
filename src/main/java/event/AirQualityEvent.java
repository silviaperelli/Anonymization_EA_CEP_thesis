package event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.util.Locale;
import utils.Writer;
import static java.lang.Integer.parseInt;

public class AirQualityEvent {

    private static final Logger logger = LoggerFactory.getLogger(AirQualityEvent.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    private LocalDateTime eventTime;
    private double coLevel; // CO(GT)
    private int pt08s1; // PT08.S1(CO)
    private int nmhc;   // NMHC(GT)
    private double c6h6;  // C6H6(GT)
    private int pt08s2; // PT08.S2(NMHC)
    private int nox;    // NOx(GT)
    private int pt08s3; // PT08.S3(NOx)
    private int no2;    // NO2(GT)
    private int pt08s4; // PT08.S4(NO2)
    private int pt08s5; // PT08.S5(O3)
    private double t;     // T
    private double rh;    // RH
    private double ah;    // AH

    public AirQualityEvent(LocalDateTime eventTime, double coLevel, int pt08s1, int nmhc, double c6h6, int pt08s2, int nox, int pt08s3, int no2, int pt08s4, int pt08s5, double t, double rh, double ah) {
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
    }

    public LocalDateTime getEventTime() {return eventTime;}
    public void setEventTime(LocalDateTime eventTime) {this.eventTime = eventTime;}

    public double getCoLevel() {return coLevel;}
    public void setCoLevel(double coLevel) {this.coLevel = coLevel;}

    public long getTimestamp() {
        return this.eventTime.toEpochSecond(ZoneOffset.UTC) * 1000;
    }

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

            double coValue = Writer.formatDouble(tokens[2]);
            // Discard missing value for the feature of interest
            if (coValue == -200) return null;

            // Parse of all the other features
            int pt08s1Val = parseInt(tokens[3]);
            int nmhcVal = parseInt(tokens[4]);
            double c6h6Val = Writer.formatDouble(tokens[5]);
            int pt08s2Val = parseInt(tokens[6]);
            int noxVal = parseInt(tokens[7]);
            int pt08s3Val = parseInt(tokens[8]);
            int no2Val = parseInt(tokens[9]);
            int pt08s4Val = parseInt(tokens[10]);
            int pt08s5Val = parseInt(tokens[11]);
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
                "%s;%s;%.6f;%d;%d;%.1f;%d;%d;%d;%d;%d;%d;%.1f;%.1f;%.4f",
                eventTime.format(DateTimeFormatter.ofPattern("dd/MM/yyyy")),
                eventTime.format(DateTimeFormatter.ofPattern("HH.mm.ss")),
                coLevel, pt08s1, nmhc, c6h6, pt08s2, nox, pt08s3, no2, pt08s4, pt08s5, t, rh, ah
        );
    }
}