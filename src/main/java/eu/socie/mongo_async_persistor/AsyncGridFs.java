/**
 * 
 */
package eu.socie.mongo_async_persistor;

import static com.allanbank.mongodb.builder.QueryBuilder.where;
import static com.allanbank.mongodb.builder.Sort.asc;

import java.io.FileNotFoundException;
import java.io.IOException;

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

	public AsyncGridFs(MongoDatabase database) {
		this(database, DEFAULT_ROOT);
	}

	public AsyncGridFs(MongoDatabase database, String rootName) {
		super(database, rootName);

		myChunksCollection = database.getCollection(rootName + CHUNKS_SUFFIX);
		myFilesCollection = database.getCollection(rootName + FILES_SUFFIX);
	}

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
					}
					else if (sink instanceof WriteStream<?>) {
						writeToStream((WriteStream<?>) sink, buffer);
					}
					else {
						throw new IOException("File contents can only be written to either a Buffer or Writestream");
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
	 * @param id is the ObjectId of the file to retrieve
	 * @param stream is the destination stream where to write to. Could be used to pump data
	 * @throws IOException will be thrown if no file was found using the id
	 */
	public void read(final ObjectId id, final WriteStream<?> stream) throws IOException {
		final Document fileDoc = myFilesCollection.findOne(where(ID_FIELD)
				.equals(id));
		if (fileDoc == null) {
			throw new FileNotFoundException(id.toString());
		}

		doRead(fileDoc, stream);
	}

	/**
	 * Read a file with ObjectId id and write the result to a buffer.
	 * @param id is the ObjectId of the file to retrieve 
	 * @param buffer is the destination of the file contents
	 * @throws IOException is thrown if no file can be read
	 */
	public void read(final ObjectId id, final Buffer buffer) throws IOException {
		final Document fileDoc = myFilesCollection.findOne(where(ID_FIELD)
				.equals(id));
		if (fileDoc == null) {
			throw new FileNotFoundException(id.toString());
		}

		doRead(fileDoc, buffer);
	}

}
