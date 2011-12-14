/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.solbase.lucenehbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermFreqVector;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.index.TermVectorMapper;
import org.apache.lucene.store.Directory;
import org.solbase.SolbaseFieldSelector;
import org.solbase.SolbaseShardUtil;
import org.solbase.SolbaseUtil;
import org.solbase.cache.CachedObjectWrapper;

public class IndexReader extends org.apache.lucene.index.IndexReader {

	private int numDocs = -1;
	
	private String indexName = new String();
	private int startDocId;
	public int getStartDocId() {
		return startDocId;
	}

	public int getEndDocId() {
		return endDocId;
	}

	private int endDocId;
	
	public static ThreadLocal<Boolean> firstPhase = new ThreadLocal<Boolean>(){
		@Override
		protected Boolean initialValue() {
			return true;
		}
	};
	
	public IndexReader(String name) {
		super();
		this.indexName = name;
		this.startDocId = SolbaseShardUtil.getStartDocId(SolbaseShardUtil.getShardNum(this.indexName));
		this.endDocId = SolbaseShardUtil.getEndDocId(startDocId);
		this.numDocs = SolbaseUtil.getSequenceId();
	}

	public synchronized IndexReader reopen() throws CorruptIndexException, IOException {
		clearCache();
		return this;
	}

	@Override
	public synchronized IndexReader reopen(boolean openReadOnly) throws CorruptIndexException, IOException {
		return reopen();
	}

	@Override
	public synchronized IndexReader reopen(IndexCommit commit) throws CorruptIndexException, IOException {
		return reopen();
	}

	private void clearCache() {

	}

	protected void doClose() throws IOException {
		clearCache();
	}

	protected void doCommit() throws IOException {
		clearCache();
	}

	protected void doDelete(int arg0) throws CorruptIndexException, IOException {

	}

	protected void doSetNorm(int arg0, String arg1, byte arg2) throws CorruptIndexException, IOException {

	}

	protected void doUndeleteAll() throws CorruptIndexException, IOException {

	}

	public int docFreq(Term term) throws IOException {
		
		TermEnum tmp = new SolbaseTermEnum(term, this.indexName, this.startDocId, this.endDocId);

		return tmp == null ? 0 : tmp.docFreq();
	}

	public Document document(int docNum, FieldSelector selector, boolean fromCache) throws CorruptIndexException,IOException{
		Document document = new Document();
		Field field = new Field("docId", docNum + "", Store.YES, Index.ANALYZED);
		document.add(field);
		return document;
	}
	
	public Document document(int docNum, FieldSelector selector) throws CorruptIndexException, IOException {
		// in sharding environment, we already have docid in term vector, it is not necessary to go
		// fetch actual doc to get this info
		// if not in sharding, we should skip this logic
		if(firstPhase.get()){
			Document doc = new Document();
			Field docId = new Field("docId",new Integer(docNum).toString(), Field.Store.YES, Field.Index.ANALYZED);
			doc.add(docId);
			
			// i need to fetch sortable field values here
			return doc;
		}
		
		try {
			if(selector != null && selector instanceof SolbaseFieldSelector){
				// TODO this logic should be more generic, currently this logic only gets called in shard initial request
				List<byte[]> fieldNames = ((SolbaseFieldSelector) selector).getFieldNames();
				if(fieldNames != null){
					String fieldNamesString = "";
					for(byte[] fieldName : fieldNames){
						fieldNamesString += Bytes.toString(fieldName);
					}
					// this will hit ShardDocument cache
					Document doc = ReaderCache.getDocument(docNum + "~" + fieldNamesString, selector, this.indexName, this.startDocId, this.endDocId).getValue();
					
					return doc;
				}
			}
			CachedObjectWrapper<Document,Long> docObj = ReaderCache.getDocument(docNum, selector, this.indexName, this.startDocId, this.endDocId);
			Document doc = docObj.getValue();

			if (selector != null && selector instanceof SolbaseFieldSelector) {
				//HTableInterface docTable = null;
				//try {
				//	docTable = SolbaseUtil.getDocTable();
				for (Integer docId : ((SolbaseFieldSelector) selector).getOtherDocsToCache()) {
					// pre-cache other ids
					// TODO: maybe need to pull HTablePool.getTable to here.
					// otherwise, each getDocument() will try to acquire
					// HTableInterface.
					ReaderCache.getDocument(docId, selector, this.indexName, this.startDocId, this.endDocId);
				}
				//} finally {
				//	SolbaseUtil.releaseTable(docTable);
				//}
			}
			
			return doc;
			
		} catch (Exception ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public Object getFieldCacheKey() {
		return ReaderCache.fieldCacheKey;
	}

	@Override
	public Collection<String> getFieldNames(FieldOption fieldOption) {
		return Arrays.asList(new String[] {});
	}

	@Override
	public TermFreqVector getTermFreqVector(int docNum, String field) throws IOException {

		TermFreqVector termVector = null;
		try {
			termVector = new org.solbase.lucenehbase.TermFreqVector(field, docNum);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return termVector;
	}

	@Override
	public void getTermFreqVector(int arg0, TermVectorMapper arg1) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void getTermFreqVector(int arg0, String arg1, TermVectorMapper arg2) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public TermFreqVector[] getTermFreqVectors(int arg0) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasDeletions() {
		return false;
	}

	@Override
	public boolean isDeleted(int arg0) {
		return false;
	}

	@Override
	public int maxDoc() {
		if(this.numDocs == -1){
			this.numDocs = SolbaseUtil.getSequenceId();
		}
		return this.numDocs;
	}

	@Override
	public byte[] norms(String field) throws IOException {
		return null;
	}

	@Override
	public void norms(String field, byte[] bytes, int offset) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int numDocs() {
		return this.maxDoc();
	}

	@Override
	public TermDocs termDocs(Term term) throws IOException {
		if (term == null)
			throw new UnsupportedOperationException();

		return super.termDocs(term);
	}

	@Override
	public TermDocs termDocs() throws IOException {
		return new SolbaseTermDocs(this.indexName);
	}

	@Override
	public TermPositions termPositions() throws IOException {
		return new SolbaseTermDocs(this.indexName);
	}

	@Override
	public TermEnum terms() throws IOException {
		return new SolbaseTermEnum(this.indexName);
	}

	@Override
	public TermEnum terms(Term term) throws IOException {
		return new SolbaseTermEnum(term, this.indexName, this.startDocId, this.endDocId);

	}

	@Override
	public Directory directory() {
		return null;
	}

	@Override
	public long getVersion() {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean isOptimized() {
		return true;
	}

	@Override
	public boolean isCurrent() {
		return true;
	}
	
    public String getIndexName() {
    	return this.indexName;
    }                                                                                                                                                               
                                                                                                                                                           
    public void setIndexName(String name) {                                                                                                                                                                
    	this.indexName = name;
    	this.startDocId = SolbaseShardUtil.getStartDocId(SolbaseShardUtil.getShardNum(this.indexName));
    	this.endDocId = SolbaseShardUtil.getEndDocId(startDocId);
    	this.numDocs = endDocId - startDocId;
    }   

}
