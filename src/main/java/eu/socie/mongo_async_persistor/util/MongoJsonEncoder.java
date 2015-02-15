/**
 * 
 */
package eu.socie.mongo_async_persistor.util;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.vertx.java.core.json.JsonObject;

/**
 * 
 * This encoder encodes JsonObjects to a String and makes sure take Mongo
 * objectIds are translated correctly
 * 
 * @author Bram Wiekens
 *
 */
public class MongoJsonEncoder {

	private static final String ID = "_id";
	private final static String ISO_DATE = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.\\d{3}(-|\\+)\\d{4}";

	private MongoJsonEncoder() {
	}

	public static String encode(JsonObject obj) {

		Map<String, Object> map = obj.toMap();

		return encode(map);
	}

	@SuppressWarnings("unchecked")
	public static String encode(Map<String, Object> map) {

		StringBuilder result = new StringBuilder("{");

		for (Entry<String, Object> entry : map.entrySet()) {
			Object val = entry.getValue();
			String key = entry.getKey();

			if (map.containsKey("$oid")) {
				String oid = (String) map.get("$oid");

				return (String.format("ObjectId(\"%s\")", oid));
			}

			if (val instanceof String) {
				if (((String) val).matches(ISO_DATE)) {
					result.append(encodeDate(key, (String) val));
				} else {
					result.append(encodeString(key, (String) val));
				}
			}

			if (val instanceof Number) {
				result.append(encodeNumber(key, (Number) val));
			}

			if (val instanceof Map<?, ?>) {
				result.append(String.format("%s : %s", key,
						encode((Map<String, Object>) val)));
			}
			
			if (val instanceof ArrayList<?>) {
				result.append(String.format("%s : [ \"abc\" ]", key));
			}

			// TODO DBref types

			// TODO other types

			result.append(',');

		}

		if (result.charAt(result.length() - 1) == ',') {
			result.deleteCharAt(result.length() - 1);
		}

		result.append("}");

		return result.toString();
	}

	/**
	 * Convert an ISO date String to a ISO date element for Mongo. Note that no
	 * timezone information is stored at the moment stored information is in UTC
	 * time.
	 * 
	 * @param key
	 *            is the key of the json element
	 * @param date
	 *            is the date string to be convert
	 * @return a ISODate encoded string
	 */
	public static String encodeDate(final String key, final String date) {

		return String.format("\"%s\" : Date(\"%s\")", key, date);

	}

	public static String encodeNumber(final String key, final Number num) {
		String result = num.toString();

		if (!result.contains(".")) {
			return String.format("\"%s\" : NumberLong(\"%s\")", key, result);
		}

		return String.format("\"%s\" : \"%s\"", key, result);

	}

	/**
	 * Encode a string to a proper bson value, if the value is an ObjectId
	 * create an ObjectId object
	 * 
	 * @param key
	 *            is the key of the json element
	 * @param val
	 *            the string value to be check and converted
	 * @return a properly encoded bson string
	 */
	private static String encodeString(final String key, String val) {
		String result = val;

		if (key.equals(ID)) {
			String idStr = (String) val;

			if (idStr.matches("-?[0-9a-fA-F]+")) {
				result = String.format("ObjectId(\"%s\")", idStr);
				return String.format("\"%s\" : %s", key, result);
			}

		}

		return String.format("\"%s\" : \"%s\"", key, result);
	}

}
