package org.solbase.indexer.mapreduce;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.List;

import javax.management.RuntimeErrorException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.index.Term;
import org.solbase.SolbaseUtil;
import org.solbase.indexer.writable.DocumentPutWritable;
import org.solbase.indexer.writable.TermDocMetadataWritable;
import org.solbase.lucenehbase.TermDocMetadataLoader;

public class SolbaseIndexReducer extends TableReducer<BytesWritable, MapWritable, Writable> {
	
	public static enum Counters {
		TOTAL_DOCS,TOTAL_TERM_VECTORS,TOTAL_TERM_VECTOR_VERSION,TOTAL_DOC_KEY_ID_MAP,TOTAL_INVALID,DUPLICATE_ROWS
	}
	
	public void reduce(BytesWritable key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		
		byte[] _key = null;
		int counter = 0;
		int dupCount = 0;
		
		// since key is checksum, we should do dedupping here
		// TODO: for now, i'm only retrieving one and ignoring rest
		boolean first = true;
		for (MapWritable writable: values){
			
			if(first){
				first = false;
				Iterator<Writable> itr = writable.keySet().iterator();

				while (itr.hasNext()) {
					BytesWritable wrtKey = (BytesWritable) itr.next();
					Writable wrt = writable.get(wrtKey);
					if (wrt instanceof DocumentPutWritable) {
						DocumentPutWritable docBytes = (DocumentPutWritable) wrt;

						String globalId = docBytes.getGlobalId();
						int docId = docBytes.getDocId();

						Put mapping = new Put(Bytes.toBytes(globalId));
						mapping.setWriteToWAL(false);
						mapping.add(Bytes.toBytes("docId"), Bytes.toBytes(""), Bytes.toBytes(docId));
						context.write(new ImmutableBytesWritable(SolbaseUtil.docKeyIdMapTable), mapping);
						context.getCounter(Counters.TOTAL_DOC_KEY_ID_MAP).increment(1);

						List<String> fieldKeys = docBytes.getFieldKeys();
						List<byte[]> fieldValues = docBytes.getFieldValues();
						List<Term> allTerms = docBytes.getAllTerms();

						byte[] md5DocId = SolbaseUtil.randomize(docId);

						Put documentPut = new Put(md5DocId);
						documentPut.setWriteToWAL(false);

						// Store each field as a column under this docId
						for (int i = 0; i < fieldKeys.size(); i++) {
							String fieldKey = fieldKeys.get(i);
							byte[] fieldValue = fieldValues.get(i);

							documentPut.add(Bytes.toBytes("field"), Bytes.toBytes(fieldKey), fieldValue);
						}

						// Finally, Store meta-data so we can delete this
						// document
						documentPut.add(Bytes.toBytes("allTerms"), Bytes.toBytes("allTerms"), SolbaseUtil.toBytes(allTerms).array());

						context.write(new ImmutableBytesWritable(SolbaseUtil.docTable), documentPut);
						context.getCounter(Counters.TOTAL_DOCS).increment(1);

						counter++;

					} else if (wrt instanceof TermDocMetadataWritable) {
						// gather all of docs given field key (field/value)
						TermDocMetadataWritable metadata = (TermDocMetadataWritable) wrt;

						// convert key to byte array
						// byte[] fieldTermKey = key.getBytes();
						byte[] termValue = metadata.getTermDocMetadata();
						_key = metadata.getFieldTermKey();

						int docId = metadata.getDocId();

						Put put = null;

						switch (TermDocMetadataLoader.storageType) {
						case KEY_ONLY: {
							put = new Put(Bytes.add(Bytes.add(_key, SolbaseUtil.delimiter, Bytes.toBytes(docId)), termValue));
							put.add(SolbaseUtil.termVectorDocColumnFamilyName, Bytes.toBytes(""), Bytes.toBytes(""));
						}
							break;
						case WIDE_ROW:
							int chunkId = TermDocMetadataLoader.getChunkId(docId);
							put = new Put(Bytes.add(_key, SolbaseUtil.delimiter, Bytes.toBytes(chunkId)));
							put.add(SolbaseUtil.termVectorDocColumnFamilyName, Bytes.toBytes(docId), termValue);
							break;
						case NARROW_ROW:
						default: {
							put = new Put(Bytes.add(_key, SolbaseUtil.delimiter, Bytes.toBytes(docId)));
							put.add(SolbaseUtil.termVectorDocColumnFamilyName, Bytes.toBytes(""), termValue);
						}
						}

						put.setWriteToWAL(false);
						context.write(new ImmutableBytesWritable(SolbaseUtil.termVectorTable), put);
						context.getCounter(Counters.TOTAL_TERM_VECTORS).increment(1);

						counter++;
					} else {
						System.out.println("else: " + writable.getClass());
						context.getCounter(Counters.TOTAL_INVALID).increment(1);
					}
				}
			} else {
				dupCount++;
			}
		}
		
		context.getCounter(Counters.DUPLICATE_ROWS).increment(dupCount);
	}
}
