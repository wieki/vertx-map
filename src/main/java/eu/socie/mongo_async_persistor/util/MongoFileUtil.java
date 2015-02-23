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

	private static final int FILE_NAME_HEADER = 128;
	private static final int CONTENT_TYPE_HEADER = 50;
	
	public static String getFilenameFromBuffer(Buffer buffer) {
		return buffer.getString(0, FILE_NAME_HEADER);
	}
	
	public static String getContentFileFromBuffer(Buffer buffer) {
		return buffer.getString(FILE_NAME_HEADER, FILE_NAME_HEADER + CONTENT_TYPE_HEADER);
	}
	
	public static Buffer getFileContentsFromBuffer(Buffer buffer) {
		// TODO this causes a copy of the buffer, can we do this smarter?		
		int start =  FILE_NAME_HEADER + CONTENT_TYPE_HEADER;
		int end = buffer.length();
		return buffer.getBuffer(start, end);
	}
	
	
	public static Buffer createHeader(String fileName, String contentType) {
		String formattedFileName = String.format("%128s", fileName);
		String formattedContentType = String.format("%50s", contentType);
		
		Buffer completedBuffer = new Buffer(formattedFileName + formattedContentType);
		
		return completedBuffer;
	}
	
	
	public static Buffer createFileBuffer(String fileName, String contentType, Buffer fileBuffer) {
		
		Buffer completedBuffer = createHeader(fileName, contentType);
		completedBuffer.appendBuffer(fileBuffer);
		
		return completedBuffer;
	}
	
}
