package org.solbase.indexer;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.solbase.SolbaseUtil;

public class SolbaseCleanupTool {
	public static void main(String[] args){
		// clean up tables
		@SuppressWarnings("deprecation")
		HBaseConfiguration conf = new HBaseConfiguration();
		conf.set("hbase.zookeeper.quorum", "den3hdptk02");
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.setInt("hbase.client.retries.number", 7);
		conf.setInt("ipc.client.connect.max.retries", 3);

		try {
			/*
			
			HTable tv = new HTable(conf, "TV");
            byte[] termBeginKey = SolbaseUtil.generateTermKey(new Term("contents", "minjun"), 419947166);
            byte[] termEndKey = SolbaseUtil.generateTermKey(new Term("contents", "minjun"), 419947168);
            Scan fieldScan = new Scan(termBeginKey, termEndKey);
            ResultScanner fieldScanner = tv.getScanner(fieldScan);

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
            */
            
			/*
			HTable sequence = new HTable(conf, "Sequence_dev");
			Get get = new Get(Bytes.toBytes("sequence"));
			Result result = sequence.get(get);
			byte[] val = result.getValue(Bytes.toBytes("id"), Bytes.toBytes(""));
			
			System.out.println(Bytes.toLong(val));
			
			*/
			/*
			
			HTable docKeyIdMap = new HTable(conf, "DocKeyIdMap_dev");

			// Bytes.toBytes(43453034);
            byte[] termBeginKey = Bytes.toBytes("13.22200883");
            byte[] termEndKey = Bytes.toBytes("13.22200883");

            //Scan fieldScan = new Scan(termBeginKey, termEndKey);
            Scan fieldScan = new Scan();
	
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("docId"), Bytes.toBytes(""), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(43453034));
            fieldScan.setFilter(filter);
            
            ResultScanner fieldScanner = docKeyIdMap.getScanner(fieldScan);

            Result docResult;
            
            
            while ((docResult = fieldScanner.next()) != null) {
                byte[] key = docResult.getRow();
                System.out.println(Bytes.toString(key));
                System.out.println(Bytes.toInt(docResult.getValue(Bytes.toBytes("docId"), Bytes.toBytes(""))));
            }
            */
        
      /*
			
            HTable docs = new HTable(conf, "Docs_dev");
            byte[] termBeginKey = Bytes.toBytes(90000);
            byte[] termEndKey = Bytes.toBytes(91000);
			

            Scan fieldScan = new Scan(termBeginKey, termEndKey);
           
            //Scan fieldScan = new Scan();
            ResultScanner fieldScanner = docs.getScanner(fieldScan);

            Result doc;
           
           while ((doc = fieldScanner.next()) != null) {
               byte[] key = doc.getRow();
               System.out.println(Bytes.toInt(key));
               //System.out.println(Bytes.toInt(docResult.getValue(Bytes.toBytes("docId"), Bytes.toBytes(""))));
          }
          System.out.println("done");
        */  
          
		
			
          	HBaseAdmin admin;
			admin = new HBaseAdmin(conf);
			truncateTable(SolbaseUtil.docKeyIdMapTable, admin);
			truncateTable(SolbaseUtil.docTable, admin);
			truncateTable(SolbaseUtil.termVectorTable, admin);
			truncateTable(SolbaseUtil.sequenceTable, admin);
			truncateTable(SolbaseUtil.termVectorVersionIDTable, admin);
            
            
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void truncateTable(byte[] tableName, HBaseAdmin admin){
		try {
			HTable table = SolbaseUtil.getLocalTable(tableName);
			HTableDescriptor descriptor = table.getTableDescriptor();
			
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			admin.createTable(descriptor);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
