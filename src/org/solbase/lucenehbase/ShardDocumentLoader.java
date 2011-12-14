package org.solbase.lucenehbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldSelector;
import org.solbase.SolbaseFieldSelector;
import org.solbase.cache.CachedObjectLoader;
import org.solbase.cache.CachedObjectWrapper;
import org.solbase.cache.LayeredCache;

public class ShardDocumentLoader implements CachedObjectLoader<String, Document, Long, Document> {

	@SuppressWarnings("unused")
	private List<byte[]> fieldNames = new ArrayList<byte[]>();

	public ShardDocumentLoader(FieldSelector selector) {
		if (selector != null && selector instanceof SolbaseFieldSelector) {
			this.fieldNames = ((SolbaseFieldSelector) selector).getFieldNames();
		}
	}

	public CachedObjectWrapper<Document, Long> loadObject(String docIdKey, int start, int end, LayeredCache<String, Document, Long, Document> cache) throws IOException {
		Document document = new Document();

		String keyField = "docId"; // TODO should probably get SchemaField from Schema object.
		Field field = new Field(keyField, this.parseDocIdFromKey(docIdKey), Store.YES, Index.ANALYZED);
		document.add(field);

		Long versionIdentifier = 0l;// TODO, get from result

		return new CachedObjectWrapper<Document, Long>(document, versionIdentifier, System.currentTimeMillis());
	}

	public Long getVersionIdentifier(String key, int startDocId, int endDocId) throws IOException {
		/*
		// TODO need to parse docId from key
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

		Long versionIdentifier = 0l;// TODO, get from result

		return versionIdentifier;
	}
	
	private String parseDocIdFromKey(String docIdKey){
		int index = docIdKey.indexOf("~");
		return docIdKey.substring(0, index);
	}

	@Override
	public void updateObject(CachedObjectWrapper<Document, Long> object, Document modificationdData, LayeredCache<String, Document, Long, Document> cache, LayeredCache.ModificationType modType, int startDocId, int endDocId) throws IOException {
		object.setValue(modificationdData);
	}

	@Override
	public void updateObjectStore(String key, Document modificationdData, IndexWriter writer, LayeredCache<String, Document, Long, Document> cache, LayeredCache.ModificationType modType, int startDocId, int endDocId) throws IOException {
		
	}
}
