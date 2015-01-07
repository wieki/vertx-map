package eu.socie.mongo_async_persistor;

/*
 * Copyright 2014 Socie
 *
 * Socie licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * @author Bram Wiekens</a>
 */

import java.util.Set;

import org.vertx.java.core.VertxException;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.platform.Verticle;

import com.allanbank.mongodb.Credential;
import com.allanbank.mongodb.MongoClient;
import com.allanbank.mongodb.MongoClientConfiguration;
import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoFactory;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.DocumentAssignable;
import com.allanbank.mongodb.bson.element.ObjectIdElement;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.builder.Find.Builder;

import eu.socie.mongo_async_persistor.util.MongoJsonEncoder;
import eu.socie.mongo_async_persistor.util.MongoUtil;

/**
 * This verticle handles requests to store data in a MongoDB instance. It use
 * the Async Mongo driver to fit right into vertx
 */
public class AsyncMongoPersistor extends Verticle {

	public static final String CONFIG_HOST = "host";
	public static final String CONFIG_PORT = "port";
	public static final String CONFIG_DATABASE_NAME = "database_name";
	public static final String CONFIG_USER = "user";
	public static final String CONFIG_PASSWORD = "password";

	public static final String DEFAULT_HOST = "localhost";
	public static final String DEFAULT_PORT = "27017";
	public static final String DEFAULT_DATABASE = "test";

	public static final String EVENT_DB_CREATE = "mongo.async.create";
	public static final String EVENT_DB_FIND = "mongo.async.find";
	public static final String EVENT_DB_UPDATE = "mongo.async.update";
	public static final String EVENT_DB_DELETE = "mongo.async.delete";

	public static final int ERROR_COLLECTION_NAME_CODE = 1001;
	public static final int ERROR_QUERY_CODE = 1002;
	public static final int ERROR_QUERY_DOCUMENT_CODE = 1003;
	public static final int ERROR_WRONG_MESSAGE_TYPE = 1004;

	public static final String QUERY_LIMIT = "limit";
	public static final String QUERY_SORT = "sort";
	public static final String QUERY_SKIP = "skip";

	// TODO consider localization
	public static final String ERROR_COLLECTION_NAME_MSG = "No collection name in query";
	public static final String ERROR_QUERY_DOCUMENT_MSG = "No query document found";
	public static final String ERROR_WRONG_TYPE_MSG = "Wrong message type, should be JSON";

	private MongoDatabase mongodb;
	private Logger log;

	public void start() {
		log = container.logger();

		JsonObject modConfig = getContainer().config();

		mongodb = connectToDatabase(modConfig);

		vertx.eventBus().registerHandler(EVENT_DB_FIND,
				(Message<JsonObject> q) -> find(q));

		vertx.eventBus().registerHandler(EVENT_DB_CREATE,
				(Message<JsonObject> q) -> save(q));

		vertx.eventBus().registerHandler(EVENT_DB_DELETE,
				(Message<JsonObject> q) -> delete(q));

		log.info("Starting Mongo Async Persistor");
	}

	/**
	 * Perform an asynchronous save on the database of a new document, if the
	 * document contains an _id the document is updated The query document
	 * should be of the following form
	 * 
	 * <pre>
	 * { 
	 * 		"collection" : "<i>name_of_collection</i>",
	 * 		"document" :  "<i>JSON document to be stored</i>"
	 * }
	 * </pre>
	 * 
	 * @see <a href="http://docs.mongodb.org/manual/core/write-concern/">MongoDB
	 *      write concerns</a>
	 * @param saveMessage
	 *            contains the document to be stored and the query parameters
	 */
	public void save(Message<JsonObject> saveMessage) {
		if (saveMessage == null)
			castError(saveMessage, ERROR_QUERY_DOCUMENT_CODE,
					ERROR_QUERY_DOCUMENT_MSG);

		JsonObject createQuery = saveMessage.body();

		String collectionName = createQuery.getString("collection");
		JsonObject saveDoc = createQuery.getObject("document");

		if (collectionName == null)
			castError(saveMessage, ERROR_COLLECTION_NAME_CODE,
					ERROR_COLLECTION_NAME_MSG);

		if (saveDoc == null)
			castError(saveMessage, ERROR_QUERY_DOCUMENT_CODE,
					ERROR_QUERY_DOCUMENT_MSG);

		String createStr = MongoJsonEncoder.encode(saveDoc);
		Document doc = Json.parse(createStr);

		MongoCollection collection = mongodb.getCollection(collectionName);

		collection.saveAsync((error, results) -> {
			if (error != null) {
				castError(saveMessage, -1, error.getMessage());
			} else {
				ObjectIdElement id = (ObjectIdElement) doc.get("_id");
				String idStr = id.getId().toHexString();
				JsonObject obj = new JsonObject();

				obj.putNumber("query_result", results);
				obj.putString("result_id", idStr);

				saveMessage.reply(obj);
			}

		}, doc);
	}

	public void update(Message<JsonObject> updateMessage) {
		// FIXME to be implemented
	}

	/**
	 * Perform an asynchronous delete on the database of an existing document,
	 * or multiple documents. The query should be of the following form
	 * 
	 * <pre>
	 * { 
	 * 		"collection" : "<i>name_of_collection</i>",
	 * 		"document" :  "<i>JSON document to be stored</i>"
	 * }
	 * </pre>
	 * 
	 * Optionally the <i>just_one</i> parameter can be set, this acts like the
	 * justOne parameter on Mongo, defaults to false
	 * 
	 * @param deleteMessage
	 *            is the delete query message
	 */
	public void delete(Message<JsonObject> deleteMessage) {
		if (deleteMessage == null)
			castError(deleteMessage, ERROR_QUERY_DOCUMENT_CODE,
					ERROR_QUERY_DOCUMENT_MSG);

		JsonObject deleteQuery = deleteMessage.body();

		String collectionName = deleteQuery.getString("collection");

		// TODO what if collection doesn't exist?
		MongoCollection collection = mongodb.getCollection(collectionName);

		JsonObject deleteDoc = deleteQuery.getObject("document");

		if (deleteDoc == null) {
			castError(deleteMessage, ERROR_QUERY_DOCUMENT_CODE,
					ERROR_QUERY_DOCUMENT_MSG);
		}

		String deleteStr = MongoJsonEncoder.encode(deleteDoc);
		Document doc = Json.parse(deleteStr);

		// Same default behavior as MongoDB
		boolean justOne = deleteQuery.getBoolean("just_one", false);

		collection.deleteAsync((error, results) -> {
			if (error != null) {
				castError(deleteMessage, -1, error.getMessage());
			} else {
				deleteMessage.reply(results);
			}
		}, doc, justOne);
	}

	/**
	 * Find will transform a JSON request document into a query suitable for
	 * MongoDB. The document should at least contain the collection name and a
	 * query document (this document may be empty)
	 * 
	 * @param findMessage
	 *            contains the query parameters and the query document.
	 */
	public void find(Message<JsonObject> findMessage) {
		if (!(findMessage.body() instanceof JsonObject)) {
			findMessage.fail(ERROR_WRONG_MESSAGE_TYPE, ERROR_WRONG_TYPE_MSG);
			throw new VertxException(ERROR_WRONG_TYPE_MSG);
		}

		JsonObject findQuery = findMessage.body();

		String collectionName = findQuery.getString("collection");

		log.debug("Accessing collection: " + collectionName);

		if (collectionName == null)
			castError(findMessage, ERROR_COLLECTION_NAME_CODE,
					ERROR_COLLECTION_NAME_MSG);

		// TODO what if collection doesn't exist?
		MongoCollection collection = mongodb.getCollection(collectionName);

		JsonObject find = findQuery.getObject("document");

		if (find == null)
			castError(findMessage, ERROR_QUERY_DOCUMENT_CODE,
					ERROR_QUERY_DOCUMENT_MSG);

		String findStr = MongoJsonEncoder.encode(find);
		
		Document doc = Json.parse(findStr);

		Find query = createFindQuery(findQuery, doc);

		collection.findAsync((error, results) -> {
			if (error != null) {
				castError(findMessage, -1, error.getMessage());
			} else {
				processFindResults(findMessage, results);
			}

		}, query);
	}

	/**
	 * 
	 * @param findQuery
	 * @param doc
	 * @return
	 */
	private Find createFindQuery(JsonObject findQuery, DocumentAssignable doc) {
		Set<String> fieldNames = findQuery.getFieldNames();

		Builder query = new Find.Builder(doc);

		for (String fieldName : fieldNames) {
			if (fieldName.equals(QUERY_LIMIT)) {
				int limit = findQuery.getInteger(QUERY_LIMIT);
				query.limit(limit);
			}
			if (fieldName.equals(QUERY_SORT)) {
				JsonObject sort = findQuery.getObject(QUERY_SORT);

				Document sortDoc = Json.parse(sort.toString());

				query.sort(sortDoc);

			}
			if (fieldName.equals(QUERY_SKIP)) {
				int skip = findQuery.getInteger(QUERY_SKIP);

				query.skip(skip);
			}

		}

		return query.build();
	}

	/**
	 * Reply an error message back to the message sender. The error will be
	 * logged.
	 * 
	 * @param msg
	 *            is the source of the message
	 * @param errorCode
	 *            the error code to send back to the end user
	 * @param errorMsg
	 *            the readable message for the end user
	 */
	private void castError(Message<?> msg, int errorCode, String errorMsg) {
		msg.fail(errorCode, errorMsg);

		log.error(errorMsg);

		throw new VertxException(errorMsg);
	}

	/**
	 * 
	 * @param message
	 *            is the request event send for processing. A reply to message
	 *            will reply send of an event.
	 * @param docs
	 *            are the resulting documents obtained from MongoDB
	 */
	private void processFindResults(Message<JsonObject> message,
			MongoIterator<Document> docs) {
		JsonArray jsonDocs = new JsonArray();

		for (Document doc : docs) {
			JsonObject obj = MongoUtil.convertBsonToJson(doc);
			jsonDocs.add(obj);
		}

		message.reply(jsonDocs);
	}

	/**
	 * Connect to a MongoDatabase. when no configuration is given default values
	 * are taken instead.
	 * 
	 * @param modConfig
	 *            is the configuration that contains about the connection, it
	 *            can contain hostname, port and host.
	 * @return a connection to a MongoDB instance
	 */
	private MongoDatabase connectToDatabase(JsonObject modConfig) {
		String host = modConfig.getString(CONFIG_HOST, DEFAULT_HOST);
		String port = modConfig.getString(CONFIG_PORT, DEFAULT_PORT);
		String database = modConfig.getString(CONFIG_DATABASE_NAME,
				DEFAULT_DATABASE);
		String username = modConfig.getString(CONFIG_USER);
		String password = modConfig.getString(CONFIG_PASSWORD);

		log.debug(String.format(
				"Connecting to database \"%s\" on port \"%s\" on host \"%s\" ",
				database, port, host));

		MongoClientConfiguration config = new MongoClientConfiguration();
		config.addServer(String.format("%s:%s", host, port));

		if (username != null && password != null) {
			Credential credentials = createCredentials(username,
					password.toCharArray(), database);
			config.addCredential(credentials);
		}

		MongoClient mongoClient = MongoFactory.createClient(config);

		MongoDatabase mongodb = mongoClient.getDatabase(database);

		return mongodb;
	}

	// FIXME This only the bare minimum of password security should at least be
	// taken from file
	private Credential createCredentials(String userName, char[] password,
			String database) {
		return Credential.builder()
				.userName(userName)
				.password(password)
				.database(database)
				.build();
	}

}