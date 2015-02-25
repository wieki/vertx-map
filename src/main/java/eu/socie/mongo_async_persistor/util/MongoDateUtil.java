package eu.socie.mongo_async_persistor.util;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class MongoDateUtil {

	public final static String ISO_DATE_REGEX = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}(-|\\+)\\d{4}";
	public static final String ISO_DATE_FORMAT ="uuuu-MM-dd'T'HH:mm:ss.SSSXX";
	
	/**
	 * Format a date according to the ISO Date format
	 * @param timeDate the time to format
	 * @return an ISO Date formatted string suitable for use in MongoDB
	 */
	public static String formatDate(OffsetDateTime timeDate) {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(ISO_DATE_FORMAT);
		
		return formatter.format(timeDate);
	}

	/**
	 * Convenience method to get an ISO formatted date, suitable for MongoDB
	 * @return an ISO Date suitable for MongoDB
	 */
	public static String nowString(){
		return formatDate(OffsetDateTime.now());

	}
	
}
