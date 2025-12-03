package utils;

import static java.lang.Double.parseDouble;

public class Writer {

    // Format double numbers and change the comma as separator for decimal number
    public static Double formatDouble(String number){
        String newNumber = number.replace(',', '.');
        double value = parseDouble(newNumber);
        // When there is a missing value, it returns Double.Nan
        if (value == -200.0) {
            return Double.NaN;
        }
        return value;
    }
}
