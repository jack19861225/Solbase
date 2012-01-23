package org.solbase.indexer.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.lucene.document.Document;
import org.solbase.SolbaseUtil;
import org.solbase.indexer.ParsedDoc;
import org.solbase.indexer.SolbaseIndexUtil;
import org.solbase.indexer.writable.DocumentPutWritable;
import org.solbase.indexer.writable.TermDocMetadataWritable;
import org.solbase.lucenehbase.TermDocMetadata;

public class SolbaseInitialIndexMapper extends TableMapper<BytesWritable, MapWritable> {
	
	private HTable docTable = null;
	private HTable docKeyIdMapTable = null;

	private Integer docId = null;
	private int idCounter = 0;
	
	private SolbaseIndexUtil indexerUtil = null;

	// counting how man rows we are indexing
	public static enum Counters {
		TOTAL_ROWS,INDEXED_ROWS,INSUFFICIENT_META_ROWS,INVALIDATED_ROWS,PRIVATE_ROWS,PARTNER_ROWS,VIDEO_ROWS,FLASH_ROWS, NEWEST_ROWS
	}

	protected void cleanup(Context context) throws IOException {
		// clean up remaining docs in buffer
		docTable.flushCommits();
		docKeyIdMapTable.flushCommits();
		SolbaseUtil.releaseTable(docTable);
		SolbaseUtil.releaseTable(docKeyIdMapTable);
	}

	protected void setup(Context context) throws IOException {
		this.docTable = (HTable) SolbaseUtil.getLocalDocTable();
		this.docKeyIdMapTable = (HTable) SolbaseUtil.getLocalDocKeyIdMapTable();
		
		String indexerUtilClassName = context.getConfiguration().get("indexerUtil");
		
		try {
			indexerUtil = (SolbaseIndexUtil) Class.forName(indexerUtilClassName).newInstance();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
		context.getCounter(Counters.TOTAL_ROWS).increment(1);
		context.setStatus(context.getCounter(Counters.TOTAL_ROWS) + "");

		// global id is user_media row key
		String globalId = Bytes.toString(row.get());
		Document doc = indexerUtil.createLuceneDocument(Bytes.toString(row.get()), values, context);

		byte[] checksum = values.getValue(Bytes.toBytes("meta"), Bytes.toBytes("checksum"));
		
		if(doc == null){
			// validation must have failed if it returned null
			return;
		}
		
		// in this case, i don't care to look up DocKeyIdMap to see if doc
		// exists already
		if (this.idCounter > (SolbaseUtil.UNIQ_ID_CHUNK - 1) || docId == null) {
			docId = SolbaseUtil.generateUniqId();
			this.idCounter = 0;
		} else {
			docId--;
		}
		
		// for us, docId is going to be global uniq id, meaning we are tied to 2 billion docs limitation
		// it doesn't really hurt to add this field to doc. and it only really matters when sharding comes in, trying to fetch docs by their docid
		indexerUtil.addFieldToDoc(doc, "docId", docId + "");

		// incrementing chunking sequence (lucene doc id)
		this.idCounter++;

		try {
			ParsedDoc parsedDoc = indexerUtil.getIndexWriter().parseDoc(doc, indexerUtil.getAnalyzer(), "", docId, indexerUtil.getSortFieldNames());

			List<TermDocMetadata> metadatas = parsedDoc.getTermDocMetadatas();
			
			MapWritable mapWritable = new MapWritable();
			DocumentPutWritable docWritable = new DocumentPutWritable(parsedDoc.getFieldsMap(), parsedDoc.getAllTerms(), docId, globalId);
			mapWritable.put(new BytesWritable(Bytes.toBytes("doc")), docWritable);

			for (TermDocMetadata metadata : metadatas) {
				byte[] key = metadata.getFieldTermKey();
				ByteBuffer buf = metadata.serialize();
				TermDocMetadataWritable writable = new TermDocMetadataWritable(docId, Bytes.toBytes(buf), key);
				mapWritable.put(new BytesWritable(key), writable);
			}
			context.write(new BytesWritable(checksum), mapWritable);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
}