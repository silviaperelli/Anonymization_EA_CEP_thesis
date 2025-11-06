package jgea.query.utils;

import event.AirQualityEvent;
import jgea.mappers.QueryRepresentation;

public class OperatorUtils {

    // Helper method that evaluates if an event satisfies a given condition for the filter operator
    public static boolean evaluateCondition(AirQualityEvent event, QueryRepresentation.FilterArgs args) {
        if (event == null) return false;

        double eventValue;
        try {
            // Switch that maps the string from the Condition to the actual event getter
            switch (args.variable()) {
                case "CO(GT)": eventValue = event.getCoLevel(); break;
                case "PT08.S1(CO)": eventValue = event.getPt08s1(); break;
                case "NMHC(GT)": eventValue = event.getNmhc(); break;
                case "C6H6(GT)": eventValue = event.getC6h6(); break;
                case "PT08.S2(NMHC)": eventValue = event.getPt08s2(); break;
                case "NOx(GT)": eventValue = event.getNox(); break;
                case "PT08.S3(NOx)": eventValue = event.getPt08s3(); break;
                case "NO2(GT)": eventValue = event.getNo2(); break;
                case "PT08.S4(NO2)": eventValue = event.getPt08s4(); break;
                case "PT08.S5(O3)": eventValue = event.getPt08s5(); break;
                case "T": eventValue = event.getT(); break;
                case "RH": eventValue = event.getRh(); break;
                case "AH": eventValue = event.getAh(); break;
                default: return false;
            }
            if(Double.isNaN(eventValue)) return false;

        } catch (Exception e) {
            return false;
        }

        double conditionValue = args.value();
        QueryRepresentation.Condition condition = args.condition();

        return switch (condition) {
            case LESS_THAN -> eventValue < conditionValue;
            case GREATER_THAN -> eventValue > conditionValue;
            case LESS_OR_EQUAL -> eventValue <= conditionValue;
            case GREATER_OR_EQUAL -> eventValue >= conditionValue;
            case EQUAL -> eventValue == conditionValue;
        };
    }

    // Helper method that applies a noise value to a specific attribute of an event
    public static AirQualityEvent applyNoise(AirQualityEvent originalEvent, String attributeToModify, double originalValue, double noise) {
        AirQualityEvent noisyEvent = new AirQualityEvent(originalEvent);

        double newValue = originalValue + noise;

        // Call the correct setter on the new instance
        switch (attributeToModify) {
            case "CO(GT)": noisyEvent.setCoLevel(newValue); break;
            case "PT08.S1(CO)": noisyEvent.setPt08s1(newValue); break;
            case "NMHC(GT)": noisyEvent.setNmhc(newValue); break;
            case "C6H6(GT)": noisyEvent.setC6h6(newValue); break;
            case "PT08.S2(NMHC)": noisyEvent.setPt08s2(newValue); break;
            case "NOx(GT)": noisyEvent.setNox(newValue); break;
            case "PT08.S3(NOx)": noisyEvent.setPt08s3(newValue); break;
            case "NO2(GT)": noisyEvent.setNo2(newValue); break;
            case "PT08.S4(NO2)": noisyEvent.setPt08s4(newValue); break;
            case "PT08.S5(O3)": noisyEvent.setPt08s5(newValue); break;
            case "T": noisyEvent.setT(newValue); break;
            case "RH": noisyEvent.setRh(newValue); break;
            case "AH": noisyEvent.setAh(newValue); break;
            default: break;
        }

        return noisyEvent;
    }

    // Helper method to obtain the value of an attribute from an event
    public static double getAttributeValue(AirQualityEvent event, String attributeName) {
        switch (attributeName) {
            case "CO(GT)": return event.getCoLevel();
            case "PT08.S1(CO)": return event.getPt08s1();
            case "NMHC(GT)": return event.getNmhc();
            case "C6H6(GT)": return event.getC6h6();
            case "PT08.S2(NMHC)": return event.getPt08s2();
            case "NOx(GT)": return event.getNox();
            case "PT08.S3(NOx)": return event.getPt08s3();
            case "NO2(GT)": return event.getNo2();
            case "PT08.S4(NO2)": return event.getPt08s4();
            case "PT08.S5(O3)": return event.getPt08s5();
            case "T": return event.getT();
            case "RH": return event.getRh();
            case "AH": return event.getAh();
            default: return Double.NaN;
        }
    }


}
