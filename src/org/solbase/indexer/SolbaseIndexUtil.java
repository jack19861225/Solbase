package org.solbase.indexer;

import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.solbase.lucenehbase.IndexWriter;

abstract public class SolbaseIndexUtil {
	
	protected Analyzer analyzer = null;

	protected IndexWriter writer = null;
	
	public Analyzer getAnalyzer(){
		return this.analyzer;
	}
	
	public IndexWriter getIndexWriter(){
		return this.writer;
	}

	abstract public List<String> getSortFieldNames();
	
	abstract public Document createLuceneDocument(String globalId, Result values, @SuppressWarnings("rawtypes") Context context);
	
	public void addFieldToDoc(Document document, String fieldId, String val){
		Field docIdField = new Field(fieldId, val, Field.Store.YES, Field.Index.NO);
		document.add(docIdField);
	}
	
	public Scan getScanner(){
		// override this method 
		Scan scan = new Scan();
	    scan.setBatch(1000);
	    scan.setCaching(1000);
	    
		return scan;
	}
	
	public String getStartTerm() {
		return null;
	}
	public String getEndTerm() {
		return null;
	}
	public Integer getNumberOfTVRegions() {
		return null;
	}	
	public Integer getMaxDocs() {
		return null;
	}
	public byte[] getStartDocKey() {
		return null;
	}
	public Integer getNumberOfDocKeyRegions() {
		return null;
	}
	public Integer getNumberOfDocRegions() {
		return null;
	}
	public byte[] getEndDocKey() {
		return null;
	}
	public String getUniqStartKey() {
		return null;
	}
	public String getUniqEndKey() {
		return null;
	}
	public Integer getNumberOfUniqRegions(){
		return null;
	}
}
