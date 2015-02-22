/**
 * 
 */
package eu.socie.mongo_async_persistor;

import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.Sort.asc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.streams.WriteStream;

import com.allanbank.mongodb.MongoCollection;
import com.allanbank.mongodb.MongoDatabase;
import com.allanbank.mongodb.MongoIterator;
import com.allanbank.mongodb.bson.Document;
import com.allanbank.mongodb.bson.Element;
import com.allanbank.mongodb.bson.NumericElement;
import com.allanbank.mongodb.bson.builder.BuilderFactory;
import com.allanbank.mongodb.bson.builder.DocumentBuilder;
import com.allanbank.mongodb.bson.element.BinaryElement;
import com.allanbank.mongodb.bson.element.ObjectId;
import com.allanbank.mongodb.builder.Find;
import com.allanbank.mongodb.gridfs.GridFs;
import com.allanbank.mongodb.util.IOUtils;

/**
 * Copyright 2015 Socie
 *
 * Socie licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Bram Wiekens
 */
public class AsyncGridFs extends GridFs {

	private final MongoCollection myChunksCollection;
	private final MongoCollection myFilesCollection;

	private static final int FILEPATH_HEADER_LENGTH = 128;
	private static final int CONTENT_TYPE_HEADER_LENGTH = 50;
	private static int HEADER_LENGTH;
	
	public static final String MIME_TYPE_FIELD = "contentType";
	
	static {
		HEADER_LENGTH = FILEPATH_HEADER_LENGTH + CONTENT_TYPE_HEADER_LENGTH;		
	}
	
	public AsyncGridFs(MongoDatabase database) {
		this(database, DEFAULT_ROOT);
	}

	public AsyncGridFs(MongoDatabase database, String rootName) {
		super(database, rootName);

		myChunksCollection = database.getCollection(rootName + CHUNKS_SUFFIX);
		myFilesCollection = database.getCollection(rootName + FILES_SUFFIX);
	}

	/*
	 * Based on the driver code
	 */
	protected void doRead(Document fileDoc, Object sink) throws IOException {
		final Element id = fileDoc.get(ID_FIELD);

		long length = -1;
		final NumericElement lengthElement = fileDoc.get(NumericElement.class,
				LENGTH_FIELD);
		if (lengthElement != null) {
			length = lengthElement.getLongValue();
		}

		long chunkSize = -1;
		final NumericElement chunkSizeElement = fileDoc.get(
				NumericElement.class, CHUNK_SIZE_FIELD);
		if (chunkSizeElement != null) {
			chunkSize = chunkSizeElement.getLongValue();
		}

		long numberChunks = -1;
		if ((0 <= length) && (0 < chunkSize)) {
			numberChunks = (long) Math.ceil((double) length
					/ (double) chunkSize);
		}

		final Element queryElement = id.withName(FILES_ID_FIELD);
		final DocumentBuilder queryDoc = BuilderFactory.start();
		queryDoc.add(queryElement);

		final Find.Builder findBuilder = new Find.Builder(queryDoc.build());
		findBuilder.setSort(asc(CHUNK_NUMBER_FIELD));

		// Small batch size since the docs are big and we can do parallel I/O.
		findBuilder.setBatchSize(2);

		long expectedChunk = 0;
		long totalSize = 0;
		final MongoIterator<Document> iter = myChunksCollection
				.find(findBuilder.build());
		try {
			for (final Document chunk : iter) {

				final NumericElement n = chunk.get(NumericElement.class,
						CHUNK_NUMBER_FIELD);
				final BinaryElement bytes = chunk.get(BinaryElement.class,
						DATA_FIELD);

				if (n == null) {
					throw new IOException("Missing chunk number '"
							+ (expectedChunk + 1) + "' of '" + numberChunks
							+ "'.");
				} else if (n.getLongValue() != expectedChunk) {
					throw new IOException("Skipped chunk '"
							+ (expectedChunk + 1) + "', retreived '"
							+ n.getLongValue() + "' of '" + numberChunks + "'.");
				} else if (bytes == null) {
					throw new IOException("Missing bytes in chunk '"
							+ (expectedChunk + 1) + "' of '" + numberChunks
							+ "'.");
				} else {
					final byte[] buffer = bytes.getValue();

					if (sink instanceof Buffer) {
						writeToBuffer((Buffer) sink, buffer);
					} else if (sink instanceof WriteStream<?>) {
						writeToStream((WriteStream<?>) sink, buffer);
					} else {
						throw new IOException(
								"File contents can only be written to either a Buffer or Writestream");
					}

					expectedChunk += 1;
					totalSize += buffer.length;
				}
			}
		} finally {
			iter.close();
		}

		if ((0 <= numberChunks) && (expectedChunk < numberChunks)) {
			throw new IOException("Missing chunks after '" + expectedChunk
					+ "' of '" + numberChunks + "'.");
		}
		if ((0 <= length) && (totalSize != length)) {
			throw new IOException("File size mismatch. Expected '" + length
					+ "' but only read '" + totalSize + "' bytes.");
		}
	}

	private void writeToBuffer(Buffer sink, byte[] buffer) {
		sink.appendBytes(buffer);
	}

	private void writeToStream(WriteStream<?> sink, byte[] buffer) {
		sink.write(new Buffer(buffer));
	}

	/**
	 * Read a file with ObjectId id and write the result to a write stream.
	 * 
	 * @param id
	 *            is the ObjectId of the file to retrieve
	 * @param stream
	 *            is the destination stream where to write to. Could be used to
	 *            pump data
	 * @throws IOException
	 *             will be thrown if no file was found using the id
	 */
	public void read(final ObjectId id, final WriteStream<?> stream)
			throws IOException {
		final Document fileDoc = myFilesCollection.findOne(where(ID_FIELD)
				.equals(id));
		if (fileDoc == null) {
			throw new FileNotFoundException(id.toString());
		}

		doRead(fileDoc, stream);
	}

	/**
	 * Read a file with ObjectId id and write the result to a buffer.
	 * 
	 * @param id
	 *            is the ObjectId of the file to retrieve
	 * @param buffer
	 *            is the destination of the file contents
	 * @throws IOException
	 *             is thrown if no file can be read
	 */
	public void read(final ObjectId id, final Buffer buffer) throws IOException {
		final Document fileDoc = myFilesCollection.findOne(where(ID_FIELD)
				.equals(id));
		if (fileDoc == null) {
			throw new FileNotFoundException(id.toString());
		}

		doRead(fileDoc, buffer);
	}

	public int readFromBuffer(byte[] target, Buffer buffer, int start) {
		int bufferSize = buffer.length();
		int size = target.length;

		if (start > bufferSize) {
			return 0;
		} else {

			int readEnd = start + size < bufferSize ? start + size : bufferSize;

			if (readEnd <= bufferSize) {
				byte[] b = buffer.getBytes(start, readEnd);

				for (int i = 0; i < b.length; i++) {
					target[i] = b[i];
				}

				return (readEnd - start);
			}
		}

		return 0;
	}

	/**
	 * The method will write a Vertx buffer to the Mongo GridFS system. The
	 * first 128 bits of the buffer are reserved for the filename of the
	 * transferred file
	 * 
	 * @param fileBuffer
	 *            contains the data of the file to be stored
	 * @return the ObjectId of the stored file
	 * @throws IOException
	 *             will be thrown when the file store operation cannot succeed
	 *             properly. Check the return message for more details.
	 */
	public ObjectId write(final Buffer fileBuffer) throws IOException {
		final ObjectId id = new ObjectId();

		boolean failed = false;
		try {
			final String filename = fileBuffer.getString(0, FILEPATH_HEADER_LENGTH).trim();
			final String contentType = fileBuffer.getString(FILEPATH_HEADER_LENGTH, FILEPATH_HEADER_LENGTH+CONTENT_TYPE_HEADER_LENGTH).trim();
			final int fileLength = fileBuffer.length() - HEADER_LENGTH;

			final byte[] buffer = new byte[DEFAULT_CHUNK_SIZE];
			final MessageDigest md5Digest = MessageDigest.getInstance("MD5");

			final List<Future<Integer>> results = new ArrayList<Future<Integer>>();
			final DocumentBuilder doc = BuilderFactory.start();
			int n = 0;
			int start = HEADER_LENGTH;

			int read = readFromBuffer(buffer, fileBuffer, start);

			while (read > 0) {

				final ObjectId chunkId = new ObjectId();

				doc.reset();
				doc.addObjectId(ID_FIELD, chunkId);
				doc.addObjectId(FILES_ID_FIELD, id);
				doc.addInteger(CHUNK_NUMBER_FIELD, n);

				final byte[] data = (read == buffer.length) ? buffer : Arrays
						.copyOf(buffer, read);
				md5Digest.update(data);
				doc.addBinary(DATA_FIELD, data);

				results.add(myChunksCollection.insertAsync(doc.build()));

				n++;
				
				read = readFromBuffer(buffer, fileBuffer, (start + n
						* buffer.length));// readFully(source,
				
			}

			doc.reset();
			doc.addObjectId(ID_FIELD, id);
			doc.addString(FILENAME_FIELD, filename);
			doc.addString(MIME_TYPE_FIELD, contentType);
			doc.addTimestamp(UPLOAD_DATE_FIELD, System.currentTimeMillis());
			doc.addInteger(CHUNK_SIZE_FIELD, buffer.length);
			doc.addLong(LENGTH_FIELD, fileLength);
			doc.addString(MD5_FIELD, IOUtils.toHex(md5Digest.digest()));

			results.add(myFilesCollection.insertAsync(doc.build()));

			// Make sure everything made it to the server.
			for (final Future<Integer> f : results) {
				f.get();
			}
		} catch (final NoSuchAlgorithmException e) {
			failed = true;
			throw new IOException(e);
		} catch (final InterruptedException e) {
			failed = true;
			final InterruptedIOException error = new InterruptedIOException(
					e.getMessage());
			error.initCause(e);
			throw error;
		} catch (final ExecutionException e) {
			failed = true;
			throw new IOException(e.getCause());
		} finally {
			if (failed) {
				myFilesCollection.delete(where(ID_FIELD).equals(id));
				myChunksCollection.delete(where(FILES_ID_FIELD).equals(id));
			}
		}

		return id;

	}

}
