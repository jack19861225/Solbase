package example;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.lucene.document.Document;
import org.solbase.indexer.SolbaseIndexUtil;

public class ExampleIndexUtil extends SolbaseIndexUtil{

	public List<String> getSortFieldNames() {
		List<String> sortFieldNames = new ArrayList<String>();
		sortFieldNames.add("price");
		
		return sortFieldNames;
	}
	
	public Document createLuceneDocument(String globalId, Result values, @SuppressWarnings("rawtypes") Context context) {
		// stub
		// only need if using initial index framework which takes input from hbase table
		Document doc = new Document();
		return doc;
	}
}