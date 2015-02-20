/*
 * Copyright 2011-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.socie.mongo_async_persistor.util;

import java.util.List;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.element.ArrayElement;
import com.allanbank.mongodb.bson.element.BooleanElement;
import com.allanbank.mongodb.bson.element.DocumentElement;
import com.allanbank.mongodb.bson.element.DoubleElement;
import com.allanbank.mongodb.bson.element.IntegerElement;
import com.allanbank.mongodb.bson.element.LongElement;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.element.StringElement;
import com.allanbank.mongodb.bson.element.TimestampElement;
import com.allanbank.mongodb.bson.json.Json;

/**
 * Utilities for converting Bson objects to and from vert.x JsonObject objects.
 * These utitilies are used in combination with the Async Mongo Driver.
 *
 * @author Jeremy Truelove
 * @author Bram Wiekens
 */
public class MongoUtil {

	//private final static String ISO_DATE = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
	
	
	/**
	 * Converts a JsonObject to a MongoDB Document
	 * 
	 * @param json
	 *            the vert.x JsonObject to convert
	 * @return the converted DBObject
	 * @throws java.lang.IllegalArgumentException
	 *             if you pass in a null object
	 */
	public static Document convertJsonToBson(JsonObject json) {
		if (json == null) {
			throw new IllegalArgumentException(
					"Cannot convert null object to DBObject");
		}

		return convertJsonToBson(json.encode());
	}

	/**
	 * Takes a Json string and converts it to a MongoDB DBObject
	 *
	 * @param json
	 *            the json string to convert into a MongoDB object
	 * @return the converted DBObject
	 * @throws java.lang.IllegalArgumentException
	 *             if you pass in a null object
	 */
	public static Document convertJsonToBson(String json) {
		if (json == null || json.equals("")) {
			throw new IllegalArgumentException(
					"Cannot convert empty string to DBObject");
		}

		return (Document) Json.parse(json);
	}

	private static JsonObject serialize(Document document) {
		List<Element> elements = document.getElements();
		JsonObject obj = new JsonObject();

		for (Element element : elements) {
			
			String name = element.getName();
		
			if (element instanceof StringElement) {
				obj.putString(name, ((StringElement) element).getValue());
			}
			if (element instanceof ObjectIdElement) {
				String id = ((ObjectIdElement) element).getId().toHexString();
				obj.putString(name, id);
			}
			if (element instanceof BooleanElement) {
				obj.putBoolean(name, ((BooleanElement) element).getValue());
			}
			if (element instanceof LongElement) {
				obj.putNumber(name, ((LongElement) element).getValue());
			}
			if (element instanceof IntegerElement) {
				obj.putNumber(name, ((IntegerElement) element).getValue());
			}
			if (element instanceof DoubleElement) {
				obj.putNumber(name, ((DoubleElement) element).getValue());
			}
			if (element instanceof DocumentElement) {
				obj.putObject(name,
						serialize(((DocumentElement) element).getDocument()));
			}
			if (element instanceof TimestampElement){
				String val = ((TimestampElement) element).getValueAsString();
				int firstQuote = val.indexOf('\'');
				int lastQuote = val.indexOf('\'', firstQuote+1);
				
				obj.putString(name, val.substring(firstQuote+1, lastQuote));
			}

			if (element instanceof ArrayElement) {
				JsonArray arr = serialize((ArrayElement) element);
				obj.putArray(name, arr);
			}

		}
		return obj;
	}

	private static JsonArray serialize(ArrayElement array) {

		JsonArray arr = new JsonArray();

		List<Element> arrElements = array.getEntries();
		for (Element element : arrElements) {
			if (element instanceof ObjectIdElement) {
				String id = ((ObjectIdElement) element).getId().toHexString();
				arr.add(id);
			}
			if (element instanceof StringElement) {
				arr.add(((StringElement) element).getValue());
			}
			if (element instanceof IntegerElement) {
				arr.add(((IntegerElement) element).getValue());
			}
			if (element instanceof DoubleElement) {
				arr.add(((DoubleElement) element).getValue());
			}
			if (element instanceof DocumentElement) {
				Document doc = (((DocumentElement) element).getDocument());
				arr.add(serialize(doc));
			}
			if (element instanceof ArrayElement) {
				arr.addArray(serialize((ArrayElement) element));
			}
		}

		return arr;
	}

	/**
	 * Converts a Bson Document to its Bson form and then encapsulates it in a
	 * JsonObject
	 * 
	 * @param dbObject
	 *            the object to convert
	 * @return the JsonObject representing the Bson MongoDB form
	 * @throws java.lang.IllegalArgumentException
	 *             if you pass in a null object
	 */
	public static JsonObject convertBsonToJson(Document document) {
		if (document == null) {
			throw new IllegalArgumentException(
					"Cannot convert null to JsonObject");
		}

		// Convert to JsonObject
		JsonObject obj = serialize(document);

		return obj;
	}

	public static JsonObject createIdReference(String id) {
		JsonObject obj = new JsonObject();

		obj.putString("$oid", id);

		return obj;

	}

}