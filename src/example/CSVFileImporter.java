package example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class CSVFileImporter {
	public static void main(String[] args){
		if(args.length < 1){
			System.out.println("Usage: java example.CSVFileImporter <csv filename>");
			System.exit(0);
		}
		@SuppressWarnings("deprecation")
		HBaseConfiguration conf = new HBaseConfiguration();
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.setInt("hbase.client.retries.number", 7);
		conf.setInt("ipc.client.connect.max.retries", 3);
		
		HTablePool hTablePool = new HTablePool(conf, 10);
		
		try {
		    BufferedReader in = new BufferedReader(new FileReader(args[0]));
		    String str;
		   
		    while ((str = in.readLine()) != null) {
		        process(str, hTablePool);
		    }
		    in.close();
		} catch (IOException e) {
		}
	}
	
	private static void process(String str, HTablePool hTablePool){	
		HTableInterface idMapTable = hTablePool.getTable("DocKeyIdMap");	
		HTableInterface seqTable = hTablePool.getTable("Sequence");
		
		try {
			StringTokenizer tokenizer = new StringTokenizer(str,",");
			if(tokenizer.countTokens() == 9){
				String id = tokenizer.nextToken();
				String cat = tokenizer.nextToken();
				String name = tokenizer.nextToken();
				float price = Float.parseFloat(tokenizer.nextToken());
				String inStock = tokenizer.nextToken();
				String author_t = tokenizer.nextToken();
				String series_t = tokenizer.nextToken();
				String sequence_i = tokenizer.nextToken();
				String genre_s = tokenizer.nextToken();
		
				Get get = new Get(Bytes.toBytes(id));
				Result result = idMapTable.get(get);
				byte[] docId = result.getValue(Bytes.toBytes("docId"), Bytes.toBytes(""));
				int docNumber = 1;
				
				SolrInputDocument doc = new SolrInputDocument();			
				if(docId != null) {
					// we've indexed this doc previously
					docNumber = Bytes.toInt(docId);
					doc.addField("edit", true);
				} else {
					docNumber = new Long(seqTable.incrementColumnValue(Bytes.toBytes("sequence"), Bytes.toBytes("id"), Bytes.toBytes(""), 1, true)).intValue();
				}
				
				doc.addField("docId", docNumber);
				doc.addField("global_uniq_id", id);
				doc.addField("cat", cat);
				doc.addField("name", name);
				doc.addField("price", new Integer((int) price).toString()); // Solbase currently do not support float embedded field comparison
				doc.addField("inStock", inStock);
				doc.addField("author_t", author_t);
				doc.addField("series_t", series_t);
				doc.addField("sequence_i", sequence_i);
				doc.addField("genre_s", genre_s);
				// whether we want to store to hbase or not
				doc.addField("updateStore", true);
			
				CommonsHttpSolrServer solbaseServer = new CommonsHttpSolrServer("http://localhost:8080/solbase/books~0");
				solbaseServer.add(doc);
			}
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			hTablePool.putTable(idMapTable);
		}
	}
}
