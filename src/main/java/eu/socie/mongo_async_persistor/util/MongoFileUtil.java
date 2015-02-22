/**
 * 
 */
package eu.socie.mongo_async_persistor.util;

import org.vertx.java.core.buffer.Buffer;

/**
 * @author Bram Wiekens
 *
 */
public class MongoFileUtil {

	public static Buffer createFileBuffer(String fileName, String contentType, Buffer fileBuffer) {
		
		String formattedFileName = String.format("%128s", fileName);
		String formattedContentType = String.format("%50s", contentType);
		
		Buffer completedBuffer = new Buffer(formattedFileName + formattedContentType);
		completedBuffer.appendBuffer(fileBuffer);
		
		
		return completedBuffer;
	}
	
}
