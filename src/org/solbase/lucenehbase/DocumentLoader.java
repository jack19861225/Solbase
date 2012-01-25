package org.solbase.lucenehbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.StringTokenizer;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.EmbeddedSortField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.EmbeddedIndexedIntField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.DocumentBuilder;
import org.solbase.SolbaseFieldSelector;
import org.solbase.SolbaseShardUtil;
import org.solbase.SolbaseUtil;
import org.solbase.cache.CachedObjectLoader;
import org.solbase.cache.CachedObjectWrapper;
import org.solbase.cache.LayeredCache;
import org.solbase.cache.LayeredCache.ModificationType;
import org.solbase.indexer.ParsedDoc;
import org.solbase.indexer.SolbaseIndexUtil;

public class DocumentLoader implements CachedObjectLoader<Integer, Document, Long, ParsedDoc> {
	private final static Logger logger = Logger.getLogger(DocumentLoader.class);
	
	private List<byte[]> fieldNames = new ArrayList<byte[]>();

	private IndexSchema schema = null;
	
	private HTableInterface docTable = null;
	
	public DocumentLoader(FieldSelector selector) {
		if (selector != null && selector instanceof SolbaseFieldSelector) {
			this.fieldNames = ((SolbaseFieldSelector) selector).getFieldNames();
		}
	}
	
	public DocumentLoader(FieldSelector selector, IndexSchema schema) {
		if (selector != null && selector instanceof SolbaseFieldSelector) {
			this.fieldNames = ((SolbaseFieldSelector) selector).getFieldNames();
		}

		this.schema = schema;
	}

	public DocumentLoader(FieldSelector selector, IndexSchema schema, HTableInterface htable) {
		this(selector, schema);
		this.docTable = htable;
	}

	public CachedObjectWrapper<Document, Long> loadObject(Integer docNum, int start, int end, LayeredCache<Integer,Document,Long,ParsedDoc> cache) throws IOException {
		Document document = new Document();

		Get documentGet = new Get(SolbaseUtil.randomize(docNum));
		if (fieldNames == null || fieldNames.size() == 0) {
			// get all columns ( except this skips meta info )
			documentGet.addFamily(Bytes.toBytes("field"));
		} else {
			for (byte[] fieldName : fieldNames) {
				documentGet.addColumn(Bytes.toBytes("field"), fieldName);
			}
		}
		
		Result documentResult = null;
		
		// if docTable is set up, reuse instance, otherwise create brand new one and close after done
		if(this.docTable == null){
			HTableInterface docTable = null;
			try {
				docTable = SolbaseUtil.getDocTable();
				documentResult = docTable.get(documentGet);
			} finally {
				SolbaseUtil.releaseTable(docTable);
			}
		} else {
			documentResult = this.docTable.get(documentGet);
		}

		if (documentResult == null || documentResult.isEmpty()) {
			return null;
		}

		Long versionIdentifier = 0l;// TODO, get from result

		NavigableMap<byte[], byte[]> familyMap = documentResult.getFamilyMap(Bytes.toBytes("field"));

		for (Map.Entry<byte[], byte[]> fieldColumn : familyMap.entrySet()) {

			Field field = null;
			String fieldName = Bytes.toString(fieldColumn.getKey());

			byte[] value;
			ByteBuffer v = ByteBuffer.wrap(fieldColumn.getValue());
			int vlimit = v.limit() + v.arrayOffset();

			if (v.array()[vlimit - 1] != Byte.MAX_VALUE && v.array()[vlimit - 1] != Byte.MIN_VALUE) {
				throw new CorruptIndexException("Solbase field is not properly encoded: " + docNum + "(" + fieldName + ")");

			} else if (v.array()[vlimit - 1] == Byte.MAX_VALUE) { // Binary
				value = new byte[vlimit - 1];
				System.arraycopy(v.array(), v.position() + v.arrayOffset(), value, 0, vlimit - 1);

				field = new Field(fieldName, value, Store.YES);
				document.add(field);
			} else if (v.array()[vlimit - 1] == Byte.MIN_VALUE) { // String
				value = new byte[vlimit - 1];
				System.arraycopy(v.array(), v.position() + v.arrayOffset(), value, 0, vlimit - 1);

				// Check for multi-fields
				String fieldString = new String(value, "UTF-8");

				if (fieldString.indexOf(Bytes.toString(SolbaseUtil.delimiter)) >= 0) {
					StringTokenizer tok = new StringTokenizer(fieldString, Bytes.toString(SolbaseUtil.delimiter));
					while (tok.hasMoreTokens()) {
						// update logic
						if(schema != null){
							SchemaField sfield = schema.getFieldOrNull(fieldName);
							
							if (sfield.getType() instanceof EmbeddedIndexedIntField) {
								EmbeddedIndexedIntField eiif = (EmbeddedIndexedIntField)sfield.getType();
								
								EmbeddedSortField sf = new EmbeddedSortField(fieldName,
																			tok.nextToken(), 
																			Field.Store.YES,
																			Field.Index.NO,
																			eiif.getFieldNumber());
								document.add(sf);
							} else {
								Field f = sfield.createField(tok.nextToken(), 1.0f);
								if (f != null) { // null fields are not added
									document.add(f);
								}
							}
							
						} else {
							field = new Field(fieldName, tok.nextToken(), Store.YES, Index.ANALYZED);
							document.add(field);
						}
					}
				} else {
					// update logic
					if(schema != null){
						SchemaField sfield = schema.getFieldOrNull(fieldName);
						
						if (sfield.getType() instanceof EmbeddedIndexedIntField) {
							EmbeddedIndexedIntField eiif = (EmbeddedIndexedIntField)sfield.getType();
							
							EmbeddedSortField sf = new EmbeddedSortField(fieldName,
																		fieldString, 
																		Field.Store.YES,
																		Field.Index.NO,
																		eiif.getFieldNumber());
							document.add(sf);
						} else {
							Field f = sfield.createField(fieldString, 1.0f);
							if (f != null) { // null fields are not added
								document.add(f);
							}
						}
						
					} else {
						field = new Field(fieldName, fieldString, Store.YES, Index.ANALYZED);
						document.add(field);
					}
				}
			}
		}

		return new CachedObjectWrapper<Document, Long>(document, versionIdentifier, System.currentTimeMillis());
	}

	public Long getVersionIdentifier(Integer key, int startDocId, int endDocId) throws IOException {
		/*
		Get documentGet = new Get(Bytes.toBytes(key));
		//TODO add appropriate column documentGet.addColumn(Bytes.toBytes("field"), fieldName);

		HTableInterface docTable = null;
		Result documentResult = null;
		try {
			docTable = SolbaseUtil.getDocTable();
			documentResult = docTable.get(documentGet);
		} finally {
			SolbaseUtil.releaseTable(docTable);
		}

		if (documentResult == null || documentResult.isEmpty()) {
			return null;
		}
		*/

		Long versionIdentifier = null;// TODO, get from result

		return versionIdentifier;
	}

	@Override
	public void updateObject(CachedObjectWrapper<Document, Long> object, ParsedDoc modificationData, LayeredCache<Integer, Document, Long, ParsedDoc> cache, LayeredCache.ModificationType modType, int startDocId, int endDocId) throws IOException {		
		if (modType == ModificationType.DELETE) {
			//don't want to delete, because someone else might still be using, just let it fall out of LRU cache
			Document oldDoc = object.getValue();
			if(oldDoc != null){
				// other cluster might have already deleted this doc and cache doesn't have this doc
				modificationData.copyFrom(deleteDocument(oldDoc, Integer.parseInt(oldDoc.getField("docId").stringValue()), modificationData.getIndexName(), modificationData.getIndexWriter(), 
											modificationData.getIndexUtil(), modificationData.getUpdateStore(), startDocId, endDocId));
			}
		} else if (modType == LayeredCache.ModificationType.UPDATE){
			Document newDoc = modificationData.getDocument();
			Document oldDoc = object.getValue();

			logger.debug("process document() call in updateObject() for docId: " + Integer.parseInt(oldDoc.getField("docId").stringValue()));
			modificationData.copyFrom(processDocument(newDoc, oldDoc, modificationData.getIndexName(), Integer.parseInt(oldDoc.getField("docId").stringValue()), modificationData.getIndexUtil(), 
										modificationData.getIndexWriter(), modificationData.getUpdateStore()));
			
			object.setValue(modificationData.getDocument());
		} else if(modType == LayeredCache.ModificationType.ADD) {
			// TODO: it should never hit here, newly added doc is obviously not going to be in cache
			Document oldDoc = object.getValue();
			logger.warn("it should never hit here, newly added doc should never be in cache: " + oldDoc.toString());
		}
	}

	@Override
	public void updateObjectStore(Integer key, ParsedDoc modificationData, IndexWriter writer, LayeredCache<Integer, Document, Long, ParsedDoc> cache, LayeredCache.ModificationType modType, int startDocId, int endDocId) throws IOException {
		Put documentPut = modificationData.getDocumentPut();
		Document doc = modificationData.getDocument();
		
		if(modType == LayeredCache.ModificationType.DELETE){
			// other cluster might have already deleted this doc and cache doesn't have this doc			
			CachedObjectWrapper<Document, Long>  cachedObj = loadObject(key, 0, 0, cache);
			if(cachedObj != null){
				Document oldDoc = cachedObj.getValue();
				int docId = Integer.parseInt(oldDoc.getField("docId").stringValue());
				modificationData.copyFrom(deleteDocument(oldDoc, docId, modificationData.getIndexName(), modificationData.getIndexWriter(), 
											modificationData.getIndexUtil(), modificationData.getUpdateStore(), startDocId, endDocId));
				
				// let's clean up Docs and DocKeyIdMap tables after deleting doc from term vector
				// we are tombstoning doc and dockeyidmap rows because of race condition in mutli clustered environment
				String globalUniqId = oldDoc.get("global_uniq_id");
				Put mappingPut = new Put(Bytes.toBytes(globalUniqId));

				mappingPut.add(SolbaseUtil.docIdColumnFamilyName, SolbaseUtil.tombstonedColumnFamilyQualifierBytes, Bytes.toBytes(1));

				writer.deleteDocument(documentPut);
				writer.updateDocKeyIdMap(mappingPut);
			}
		} else if(modType == LayeredCache.ModificationType.UPDATE){
			// TODO: might want to refactor this logic
			// we are currently always loading Doc before calling updateObject.
			// within updateObject, it should always call processDocument() which will set documentPut obj
			writer.updateDocument(documentPut, modificationData.getDocument());
		} else if(modType == LayeredCache.ModificationType.ADD){
			writer.addDocument(documentPut, doc);
		}
	}
	
	private ParsedDoc deleteDocument(Document oldDoc, int docId, String indexName, IndexWriter writer, SolbaseIndexUtil indexUtil, boolean updateStore, int startDocId, int endDocId){
		try {
			// clone so read won't conflict
			oldDoc = new Document(oldDoc);
			oldDoc.removeField("docId");
			ParsedDoc parsedDoc = writer.parseDoc(oldDoc, schema.getAnalyzer(), indexName, docId, indexUtil.getSortFieldNames());

			List<TermDocMetadata> metadatas = parsedDoc.getTermDocMetadatas();

			// TODO: doing duplicate work here - once from updateObject and again from updateObjectStore
			for (TermDocMetadata metadata : metadatas) {
				ReaderCache.updateTermDocsMetadata(metadata.getTerm(), metadata, indexName, writer, LayeredCache.ModificationType.DELETE, updateStore, startDocId, endDocId);
			}
			
			return parsedDoc;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MemcachedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private ParsedDoc processDocument(Document newDoc, Document oldDoc, String indexName, int docNumber, SolbaseIndexUtil indexUtil, IndexWriter writer, boolean updateStore){

		try {
			@SuppressWarnings("unchecked")
			List<Fieldable> newFields = newDoc.getFields();
			boolean termVectorChanged = false;

			for (Fieldable field : newFields) {
				if (field.isIndexed() || field instanceof EmbeddedSortField) {
					termVectorChanged = true;
					break;
				}
			}

			// one of field value that's indexed has changed. so we need to
			// do diff on terms
			if (termVectorChanged) {
				Field docIdField = oldDoc.getField("docId");
				
				// cloning old doc, so it won't conflict with read
				oldDoc = new Document(oldDoc);
				oldDoc.removeField("docId");

				// parsing old doc to get all terms
				try {
					ParsedDoc oldParsedDoc = writer.parseDoc(oldDoc, schema.getAnalyzer(), indexName, docNumber, indexUtil.getSortFieldNames());

					List<Term> oldTerms = oldParsedDoc.getAllTerms();
					List<TermDocMetadata> oldTermDocMetas = oldParsedDoc.getTermDocMetadatas();

					Document mergedDoc = mergeOldAndNew(oldDoc, newDoc);
					ParsedDoc parsedDoc = writer.parseDoc(mergedDoc, schema.getAnalyzer(), indexName, docNumber, indexUtil.getSortFieldNames());

					List<TermDocMetadata> newTermDocMetas = parsedDoc.getTermDocMetadatas();
					List<Term> newTerms = parsedDoc.getAllTerms();

					List<Term> updateList = new ArrayList<Term>(oldTerms);
					List<Term> deleteList = new ArrayList<Term>(oldTerms);
					List<Term> addList = new ArrayList<Term>(newTerms);

					Collections.copy(updateList, oldTerms);
					Collections.copy(deleteList, oldTerms);
					Collections.copy(addList, newTerms);

					updateList.retainAll(newTerms);
					deleteList.removeAll(newTerms);
					addList.removeAll(oldTerms);
					int shardNum = SolbaseShardUtil.getShardNum(indexName);
					int startDocId = SolbaseShardUtil.getStartDocId(shardNum);
					int endDocId = SolbaseShardUtil.getEndDocId(shardNum);
					// updating tv first
					for (TermDocMetadata termDocMeta : newTermDocMetas) {
						Term term = termDocMeta.getTerm();
						if (updateList.contains(term)) {
							logger.debug("updating this term: " + term.toString());
							ReaderCache.updateTermDocsMetadata(term, termDocMeta, indexName, writer, LayeredCache.ModificationType.UPDATE, updateStore, startDocId, endDocId);
						} else if (addList.contains(term)) {
							ReaderCache.updateTermDocsMetadata(term, termDocMeta, indexName, writer, LayeredCache.ModificationType.ADD, updateStore, startDocId, endDocId);
						}
					}

					// clean up deletes
					if (deleteList.size() > 0) {
						for (TermDocMetadata termDocMeta : oldTermDocMetas) {
							Term term = termDocMeta.getTerm();

							if (deleteList.contains(term)) {
								ReaderCache.updateTermDocsMetadata(term, termDocMeta, indexName, writer, LayeredCache.ModificationType.DELETE, updateStore, startDocId, endDocId);

							}
						}
					}

					parsedDoc.getDocument().add(docIdField);

					return parsedDoc;
				} catch (NullPointerException e) {
					return null;
				}
			} else {
				Document mergedDoc = mergeOldAndNew(oldDoc, newDoc);

				ParsedDoc parsedDoc = writer.parseDoc(mergedDoc, schema.getAnalyzer(), indexName, docNumber, indexUtil.getSortFieldNames());

				return parsedDoc;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MemcachedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private Document mergeOldAndNew(Document oldDoc, Document newDoc){
		SolrInputDocument newInputDoc = new SolrInputDocument();

		@SuppressWarnings("unchecked")
		List<Fieldable> newFields = newDoc.getFields();
		for (Fieldable field : newFields) {
			String fieldName = field.name();
			String fieldValue = field.stringValue();
			newInputDoc.addField(fieldName, fieldValue);
			oldDoc.removeField(fieldName);
		}
		@SuppressWarnings("unchecked")
		List<Fieldable> oldFields = oldDoc.getFields();
		for (Fieldable field : oldFields) {
			String fieldName = field.name();
			String fieldValue = field.stringValue();
			newInputDoc.addField(fieldName, fieldValue);
		}
		
		Document mergedDoc = DocumentBuilder.toDocument(newInputDoc, schema);
		
		return mergedDoc;
	}
}
