package org.solbase.indexer;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.solbase.SolbaseUtil;
import org.solbase.lucenehbase.TermDocMetadata;

public class SolbaseIndexDebuggingTool {
	public static void main(String[] args){
		// clean up tables
		@SuppressWarnings("deprecation")
		HBaseConfiguration conf = new HBaseConfiguration();
		conf.set("hbase.zookeeper.quorum", "den3db099");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.setInt("hbase.client.retries.number", 7);
		conf.setInt("ipc.client.connect.max.retries", 3);

		try {
			/*
			HTable userMedia = new HTable(conf, "user_media");
			Get get = new Get(Bytes.toBytes("0106207227:0003417627:DSC05376.jpg"));
			
			Result result = userMedia.get(get);
			
			PBIndexUtil indexerUtil = new PBIndexUtil();
			Document doc = indexerUtil.createLuceneDocument(null, result, null);
			ParsedDoc parsedDoc = indexerUtil.getIndexWriter().parseDoc(doc, indexerUtil.getAnalyzer(), "", 1, indexerUtil.getSortFieldNames());
			List<TermDocMetadata> metadatas = parsedDoc.getTermDocMetadatas();
			for (TermDocMetadata metadata : metadatas) {
				
				ByteBuffer buf = metadata.serialize();
				byte[] key = metadata.getFieldTermKey();
			}
			*/
			
			HTable tv = new HTable(conf, "TV");
	        byte[] termBeginKey = SolbaseUtil.generateTermKey(new Term("contents", "minjun"), 419947166);
	        byte[] termEndKey = SolbaseUtil.generateTermKey(new Term("contents", "minjun"), 419947169);
	        Scan fieldScan = new Scan(termBeginKey, termEndKey);
	        ResultScanner fieldScanner;
			fieldScanner = tv.getScanner(fieldScan);
	        Result docResult;
	        
	        while ((docResult = fieldScanner.next()) != null) {
	            byte[] key = docResult.getRow();
	            byte[] val = new byte[key.length - 26];
	            for(int i = 0; i < val.length; i++){
	            	val[i] = key[26+i];
	            }
	            TermDocMetadata meta = new TermDocMetadata(0, val);
	            
	            System.out.println(Bytes.toString(key));
	        } 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
}
