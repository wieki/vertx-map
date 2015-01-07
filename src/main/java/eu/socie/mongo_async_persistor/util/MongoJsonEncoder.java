/**
 * 
 */
package eu.socie.mongo_async_persistor.util;

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

	private MongoJsonEncoder() {
	}

	public static String encode(JsonObject obj) {
		
		Map<String, Object> map = obj.toMap();
	
		return encode(map);
	}
		
	@SuppressWarnings("unchecked")
	public static String encode(Map<String,Object> map) {
	
		StringBuilder result = new StringBuilder("{");
		
		

		for (Entry<String, Object> entry : map.entrySet()) {
			Object val = entry.getValue();
			String key = entry.getKey();
			
			if (map.containsKey("$oid")) {
				String oid = (String) map.get("$oid");
				
				return (String.format("ObjectId(\"%s\")",oid));
			}			
			
			//System.out.println(key + " "+ val.getClass());

			if (val instanceof String) {
				result.append(encodeString(key, (String) val));
			}

			if (val instanceof Number) {
				result.append(encodeNumber(key, (Number) val));
			}
			
			if (val instanceof Map<?,?>) {
				result.append(String.format("%s : %s", key, encode((Map<String,Object>)val)));
			}

			// TODO Date types

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

	public static String encodeNumber(String key, Number num) {
		String result = num.toString();

		if (!result.contains(".")) {
			return String.format("\"%s\" : NumberLong(\"%s\")", key, result);
		}

		return String.format("\"%s\" : \"%s\"", key, result);

	}

	/**
	 * Encode a string to a proper bson value, if the value is an ObjectId create an ObjectId object 
	 * @param key is the key of the json element
	 * @param val the string value to be check and converted
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
