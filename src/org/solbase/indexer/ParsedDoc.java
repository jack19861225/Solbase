package org.solbase.indexer;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.solbase.lucenehbase.IndexWriter;
import org.solbase.lucenehbase.TermDocMetadata;

public class ParsedDoc implements Serializable {

	private static final long serialVersionUID = 981453389480294959L;
	
	private List<TermDocMetadata> metadatas;
	private Put documentPut = null;
	private Document doc;
	private Set<Map.Entry<String, byte[]>> fieldsMap;
	private List<Term> allTerms;
	private String indexName;
	private SolbaseIndexUtil indexUtil;
	private boolean updateStore;
	private IndexWriter indexWriter;
	
	public ParsedDoc(){
		// used for delete
	}
	
	public ParsedDoc(Document newDoc){
		// used for update
		this.doc = newDoc;
	}
	
	public ParsedDoc(List<TermDocMetadata> metadatas, Document doc, Put documentPut, Set<Map.Entry<String, byte[]>> fieldsMap, List<Term> allTerms){
		this.metadatas = metadatas;
		this.documentPut = documentPut;
		this.doc = doc;
		this.fieldsMap = fieldsMap;
		this.allTerms = allTerms;
	}
	
	public void copyFrom(ParsedDoc doc){
		this.metadatas = doc.metadatas;
		this.documentPut = doc.documentPut;
		this.doc = doc.doc;
		this.fieldsMap = doc.fieldsMap;
		this.allTerms = doc.allTerms;
	}
	
	public List<TermDocMetadata> getTermDocMetadatas(){
		return this.metadatas;
	}
	
	public Put getDocumentPut(){
		return this.documentPut;
	}
	
	public Set<Map.Entry<String, byte[]>> getFieldsMap(){
		return this.fieldsMap;
	}
	
	public List<Term> getAllTerms(){
		return this.allTerms;
	}
	
	public Document getDocument(){
		return doc;
	}
	
	public void setIndexName(String indexName){
		this.indexName = indexName;
	}
	
	public String getIndexName(){
		return this.indexName;
	}
	
	public void setIndexUtil(SolbaseIndexUtil indexUtil){
		this.indexUtil = indexUtil;
	}
	
	public SolbaseIndexUtil getIndexUtil(){
		return this.indexUtil;
	}

	public void setUpdateStore(boolean updateStore){
		this.updateStore = updateStore;
	}
	
	public boolean getUpdateStore(){
		return this.updateStore;
	}
	
	public void setIndexWriter(IndexWriter indexWriter){
		this.indexWriter = indexWriter;
	}
	
	public IndexWriter getIndexWriter(){
		return this.indexWriter;
	}
}
