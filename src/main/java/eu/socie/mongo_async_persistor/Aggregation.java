/**
 * 
 */
package eu.socie.mongo_async_persistor;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.json.Json;
import com.allanbank.mongodb.builder.Aggregate;
import com.allanbank.mongodb.builder.Aggregate.Builder;

import edu.umd.cs.findbugs.annotations.NonNull;
import eu.socie.mongo_async_persistor.util.MongoUtil;



/**
 * @author Bram Wiekens
 *	
 *	Provide aggregation functions
 */
public class Aggregation {

	
	private MongoDatabase mongodb;
	
	public Aggregation(MongoDatabase mongodb) {
		this.mongodb = mongodb;
	}
	

	public void aggregate(Message<JsonObject> aggregationMessage) {
		JsonObject aggregationQuery = aggregationMessage.body();
		String collectionName = aggregationQuery.getString("collection");

		// TODO what if collection doesn't exist?
		MongoCollection collection = mongodb.getCollection(collectionName);
		
		JsonObject group = aggregationQuery.getObject("group");
		
		if (group == null) {
			// FIXME reply error
		}
		
		//String findStr = MongoJsonEncoder.encode(find);
		
		Document groupDoc = Json.parse(group.toString());
		
		Aggregate agg = createAggregation(groupDoc);
		
		MongoIterator<Document> it = collection.aggregate(agg);
		
		aggregationMessage.reply(convertResults(it));
	}
	
	private JsonArray convertResults(MongoIterator<Document> it) {
		JsonArray results = new JsonArray();
		
		it.forEach((doc) -> results.add(MongoUtil.convertBsonToJson(doc)));
		
		return results;
	}
	
	
	private Aggregate createAggregation(@NonNull Document groupDoc) {
		Builder builder = Aggregate.builder().group(groupDoc);
		return builder.build();
	}
}
