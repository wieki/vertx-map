/**
 * 
 */
package eu.socie.mongo_async_persistor.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

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
	private final static String MONGO_ID = "^([a-z0-9]){24}$";

	private MongoJsonEncoder() {
	}

	public static String encode(JsonObject obj) {

		Map<String, Object> map = obj.toMap();

		return encode(map);
	}

	private static boolean isId(String key, String value) {
		if (key.endsWith("_id")) {
			return Pattern.matches(MONGO_ID, value);
		}

		return false;
	}

	@SuppressWarnings("unchecked")
	public static String encode(Map<String, Object> map) {

		StringBuilder result = new StringBuilder("{");
		ArrayList<String> stringParts = new ArrayList<String>();
		
		for (Entry<String, Object> entry : map.entrySet()) {
			Object val = entry.getValue();
			String key = entry.getKey();

			if (val instanceof String) {
				// Assume that fields that end on _id are ObjectId references,
				// then check if assumption is true
				if (isId(key, (String) val)) {
					stringParts.add(encodeId(key, (String) val));
				} else if (((String) val).matches(MongoDateUtil.ISO_DATE)) {
					stringParts.add(encodeDate(key, (String) val));
				} else {
					stringParts.add(encodeString(key, (String) val));
				}
			}
			
			if (val instanceof Boolean) {
				stringParts.add(encodeBoolean(key, (Boolean) val));
			}
			
			if (val instanceof Number) {
				stringParts.add(encodeNumber(key, (Number) val));
			}

			if (val instanceof Map<?, ?>) {
				stringParts.add(String.format("%s : %s", key,
						encode((Map<String, Object>) val)));
			}

			if (val instanceof ArrayList<?>) {
				if (!((ArrayList<?>) val).isEmpty()){
				
				String encodeStr = encodeArray((ArrayList<?>) val);

				stringParts.add(String.format("%s : [ %s ]", key, encodeStr));
				}
			}

			// TODO DBref types

			// TODO other types
		}
		
		String resultStr = String.join(",", stringParts); 
		
		result.append(resultStr);

		if (result.charAt(result.length() - 1) == ',') {
			result.deleteCharAt(result.length() - 1);
		}

		result.append("}");

		return result.toString();
	}


	@SuppressWarnings("unchecked")
	private static String encodeArray(ArrayList<?> array) {
		Iterator<?> iterator = array.iterator();

		Object first = iterator.next();

		// TODO Add support for list of ObjectId's

		if (first != null && first instanceof String) {

			if (!iterator.hasNext()) {
				return first == null ? "" : first.toString();
			}

			return encodeStringArray(first.toString(), iterator);
		}

		if (first != null && first instanceof Number) {

			if (!iterator.hasNext()) {
				return first == null ? "" : first.toString();
			}

			return encodeNumberArray((Number) first, iterator);
		}

		if (first != null && first instanceof Map<?, ?>) {

			if (!iterator.hasNext()) {
				return first == null ? "" : encode((Map<String, Object>) first);
			}

			return encodeJsonObjectArray((Map<?, ?>) first, iterator);
		}

		return "";

	}

	@SuppressWarnings("unchecked")
	private static String encodeJsonObjectArray(Map<?, ?> first,
			Iterator<?> iterator) {

		StringBuffer buf = new StringBuffer(256);

		if (first != null) {
			buf.append(encode((Map<String, Object>) first));
		}

		while (iterator.hasNext()) {
			buf.append(',');
			Map<String, Object> obj = (Map<String, Object>) iterator.next();
			if (obj != null) {
				buf.append(encode(obj));
			}
		}

		return buf.toString();
	}

	private static String encodeNumberArray(Number first, Iterator<?> iterator) {
		// two or more elements
		StringBuffer buf = new StringBuffer(256);

		if (first != null) {
			buf.append(first.toString());
		}

		while (iterator.hasNext()) {
			buf.append(',');
			Number obj = (Number) iterator.next();
			if (obj != null) {
				buf.append(obj.toString());
			}
		}

		return buf.toString();

	}

	private static String encodeStringArray(String first, Iterator<?> iterator) {

		// two or more elements
		StringBuffer buf = new StringBuffer(256); // Java default is 16,
													// probably too small
		if (first != null) {
			buf.append(first);
		}

		while (iterator.hasNext()) {
			buf.append(',');
			Object obj = iterator.next();
			if (obj != null) {
				buf.append(obj);
			}
		}

		return buf.toString();

	}

	private static String encodeBoolean(final String key, final Boolean bool){
		return String.format("\"%s\" : %s", key, bool.toString());
	}
	
	/**
	 * Convert an String ID to a id element for Mongo.
	 * 
	 * @param key
	 *            is the key of the json element
	 * @param date
	 *            is the date string to be convert
	 * @return a ObjectId encoded string
	 */
	public static String encodeId(final String key, final String id) {

		return String.format("\"%s\" : ObjectId(\"%s\")", key, id);
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

		return String.format("\"%s\" : ISODate(\"%s\")", key, date);

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
