package eu.modaclouds.utils;

import java.text.SimpleDateFormat;
import java.text.ParsePosition;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;

public class DateUtils {
	/** used to format time zone offset */
    private static DecimalFormat xxFormat = new DecimalFormat("00");

    /**
     * specific ISO8601-compliant format string (without time zone information)
     */
    private static String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    /**
     * Parses a ISO8601-compliant date/time string.
     * @param text the date/time string to be parsed
     * @return a <code>Calendar</code>, or <code>null</code>
     * if the input could not be parsed
     */
    public static Calendar parse(String text) {
        // parse time zone designator (Z or +00:00 or -00:00)
        // and build time zone id (GMT or GMT+00:00 or GMT-00:00)
        String tzID = "GMT";	// Zulu, i.e. UTC/GMT (default)
        int tzPos = text.indexOf('Z');
        if (tzPos == -1) {
            // not Zulu, try +
            tzPos = text.indexOf('+');
            if (tzPos == -1) {
                // not +, try -, but remember it might be used within first
                // 8 charaters for separating year, month and day, yyyy-mm-dd
                tzPos = text.indexOf('-', 8);
            }
            if (tzPos == -1) {
                // no time zone specified, assume Zulu
            } else {
                // offset to UTC specified in the format +00:00/-00:00
                tzID += text.substring(tzPos);
                text = text.substring(0, tzPos);
            }
        } else {
            // Zulu, i.e. UTC/GMT
            text = text.substring(0, tzPos);
        }

        TimeZone tz = TimeZone.getTimeZone(tzID);
        SimpleDateFormat format = new SimpleDateFormat(ISO_FORMAT);
        format.setLenient(false);
        format.setTimeZone(tz);
        Date date = format.parse(text, new ParsePosition(0));
        if (date == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance(tz);
        cal.setTime(date);
        return cal;
    }

    /**
     * Formats a <code>Calendar</code> value into a ISO8601-compliant
     * date/time string.
     * @param cal the time value to be formatted into a date/time string.
     * @return the formatted date/time string.
     */
    public static String format(Calendar cal) {
        SimpleDateFormat format =
                new SimpleDateFormat(ISO_FORMAT);
        TimeZone tz = cal.getTimeZone();
        format.setTimeZone(tz);

        StringBuffer tzd = new StringBuffer("Z");
        int offset = tz.getRawOffset();
        if (offset != 0) {
            int hours = Math.abs((offset / (60 * 1000)) / 60);
            int minutes = Math.abs((offset / (60 * 1000)) % 60);
            tzd.append(offset < 0 ? "-" : "+");
            tzd.append(xxFormat.format(hours));
            tzd.append(":");
            tzd.append(xxFormat.format(minutes));
        }
        return format.format(cal.getTime()) + tzd.toString();
    }
    
    /**
     * Formats a <code>Date</code> value into a ISO8601-compliant
     * date/time string.
     * @param date the time value to be formatted into a date/time string.
     * @return the formatted date/time string.
     */
    public static String format(Date date) {
    	Calendar cal=Calendar.getInstance();
    	cal.setTime(date);
    	return format(cal);
    }
}
