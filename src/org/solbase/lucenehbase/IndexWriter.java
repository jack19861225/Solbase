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
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.EmbeddedSortField;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Similarity;
import org.solbase.SolbaseUtil;
import org.solbase.indexer.ParsedDoc;
import org.solbase.indexer.SolbaseIndexUtil;

public class IndexWriter {
	private static final Logger logger = Logger.getLogger(IndexWriter.class);
	
	private static final InheritableThreadLocal<String> indexName = new InheritableThreadLocal<String>();

	private static SolbaseIndexUtil indexUtil;

	private Similarity similarity = Similarity.getDefault(); // how to
																// normalize;
	// going to hold onto puts until later
	public List<Put> puts = new ArrayList<Put>();

	// private static final Logger logger = Logger.getLogger(IndexWriter.class);

	public IndexWriter() {

	}

	public IndexWriter(String indexName) {
		setIndexName(indexName);
	}
	
	public void setIndexUtil(SolbaseIndexUtil indexUtil){
		this.indexUtil = indexUtil;
	}
	
	public void addDocument(Put documentPut, Document doc){
		byte[] docId = documentPut.getRow();
		String uniqId =doc.get("global_uniq_id");
		
		if (uniqId != null && docId != null) {
			// for remote server update via solr update, we want to use
			// getDocTable(), but for now map/red can use local htable
			HTableInterface docTable = SolbaseUtil.getDocTable();
			// insert document to doctable
			try {
				documentPut.add(SolbaseUtil.timestampColumnFamilyName, SolbaseUtil.tombstonedColumnFamilyQualifierBytes, Bytes.toBytes(0));
				docTable.put(documentPut);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {		
				SolbaseUtil.releaseTable(docTable);
			}
			
			// need to insert to docKeyIdMap
			Put mapping = new Put(Bytes.toBytes(uniqId));
			mapping.add(Bytes.toBytes("docId"), Bytes.toBytes(""), SolbaseUtil.randomize(docId));
			mapping.add(SolbaseUtil.docIdColumnFamilyName, SolbaseUtil.tombstonedColumnFamilyQualifierBytes, Bytes.toBytes(0));
			updateDocKeyIdMap(mapping);

		} else {
			if(uniqId == null){
				logger.info("uniqId is null: " + doc.toString());	
			} else if(docId == null){
				logger.info("docId is null: " + doc.toString());	
			} else {
				logger.info("both uniqId and docId are null: " + doc.toString());	
			}
		}
	}
	public void updateDocument(Put documentPut, Document doc){
		String uniqId = doc.get("global_uniq_id");
		Put mappingPut = new Put(Bytes.toBytes(uniqId));
		mappingPut.add(SolbaseUtil.docIdColumnFamilyName, SolbaseUtil.tombstonedColumnFamilyQualifierBytes, Bytes.toBytes(0));
		updateDocKeyIdMap(mappingPut);

		// for remote server update via solr update, we want to use
		// getDocTable(), but for now map/red can use local htable
		HTableInterface docTable = SolbaseUtil.getDocTable();
		// insert document to doctable
		try {
			documentPut.add(SolbaseUtil.timestampColumnFamilyName, SolbaseUtil.tombstonedColumnFamilyQualifierBytes, Bytes.toBytes(0));
			docTable.put(documentPut);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {		
			SolbaseUtil.releaseTable(docTable);
		}
	}
	
	public void deleteDocument(Put documentPut){
		// for remote server update via solr update, we want to use
		// getDocTable(), but for now map/red can use local htable
		HTableInterface docTable = SolbaseUtil.getDocTable();
		// insert document to doctable
		try {
			Delete delete = new Delete(documentPut.getRow());
			docTable.delete(delete);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {		
			SolbaseUtil.releaseTable(docTable);
		}
	}
	
	public void updateDocKeyIdMap(Put docKeyIdPut){
		// for remote server update via solr update, we want to use
		// getDocTable(), but for now map/red can use local htable
		HTableInterface docKeyIdMap = SolbaseUtil.getDocKeyIdMapTable();
		// insert document to doctable
		try {
			docKeyIdMap.put(docKeyIdPut);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {		
			SolbaseUtil.releaseTable(docKeyIdMap);
		}
	}
	
	public void deleteDocKeyIdMap(Put mappingPut){
		// for remote server update via solr update, we want to use
		// getDocTable(), but for now map/red can use local htable
		HTableInterface mappingTable = SolbaseUtil.getDocKeyIdMapTable();
		// insert document to doctable
		try {
			Delete delete = new Delete(mappingPut.getRow());
			mappingTable.delete(delete);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {		
			SolbaseUtil.releaseTable(mappingTable);
		}		
	}

	public void addTermVector(TermDocMetadata termDocMeta, int startDocId, int endDocId) throws CorruptIndexException, IOException {
		// getting terVector and doc tables
		HTableInterface termVectorTable = SolbaseUtil.getTermVectorTable();
		try {

			byte[] key = termDocMeta.getFieldTermKey();
			ByteBuffer buf = termDocMeta.serialize();

			int docNumber = termDocMeta.getDocId();
			
			Put put = null;

			switch (TermDocMetadataLoader.storageType) {
			case KEY_ONLY: {
				put = new Put(Bytes.add(Bytes.add(key, SolbaseUtil.delimiter, Bytes.toBytes(docNumber)), Bytes.toBytes(buf)));
				put.add(SolbaseUtil.termVectorDocColumnFamilyName, Bytes.toBytes(""), Bytes.toBytes(""));
			}
				break;
			case WIDE_ROW:
				int chunkId = TermDocMetadataLoader.getChunkId(docNumber);
				put = new Put(Bytes.add(key, SolbaseUtil.delimiter, Bytes.toBytes(chunkId)));
				put.add(SolbaseUtil.termVectorDocColumnFamilyName, Bytes.toBytes(docNumber), Bytes.toBytes(buf));
				break;
			case NARROW_ROW:
			default: {
				put = new Put(Bytes.add(key, SolbaseUtil.delimiter, Bytes.toBytes(docNumber)));
				put.add(SolbaseUtil.termVectorDocColumnFamilyName, Bytes.toBytes(""), Bytes.toBytes(buf));
			}
			}
			
			logger.info(Bytes.toString(Bytes.add(Bytes.add(key, SolbaseUtil.delimiter, Bytes.toBytes(docNumber)), Bytes.toBytes(buf))));
			logger.info("updating this term: " + termDocMeta.getTerm().toString() + " and docId: " + termDocMeta.docId);

			termVectorTable.put(put);
		} finally {
			SolbaseUtil.releaseTable(termVectorTable);
		}
	}

	public void updateTermVector(TermDocMetadata termDocMeta, int startDocId, int endDocId){
		// to update, we should first delete existing term doc meta data.
		// getting terVector and doc tables

		try {
			deleteTermVector(termDocMeta, startDocId, endDocId);
			addTermVector(termDocMeta, startDocId, endDocId);
		} catch (CorruptIndexException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public void updateTermVectorVersionId(TermDocMetadata termDocMeta, int startDocId, int endDocId){
		HTableInterface versionIdTable = SolbaseUtil.getTermVectorVersionIDTable();
		
		Term term = termDocMeta.getTerm();
		byte[] fieldTermKey = SolbaseUtil.generateTermKey(term);
		
		Put updatePut = new Put(Bytes.add(fieldTermKey, Bytes.toBytes(startDocId), Bytes.toBytes(endDocId)));
		updatePut.setWriteToWAL(false);
		if(termDocMeta.versionIdentifier == null){
			// we havn't loaded this term into cache yet, but need to do update with
			try {
				TermDocMetadataVersionIdentifier versionIdentifier = TermDocMetadataLoader.getStaticVersionIdentifier(term, startDocId, endDocId);
				updatePut.add(SolbaseUtil.timestampColumnFamilyName, Bytes.toBytes(""), Bytes.toBytes(versionIdentifier.getVersionIdentifier()));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else {
			updatePut.add(SolbaseUtil.timestampColumnFamilyName, Bytes.toBytes(""), Bytes.toBytes(termDocMeta.versionIdentifier.getVersionIdentifier()));
		}
		try {
			versionIdTable.put(updatePut);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			SolbaseUtil.releaseTable(versionIdTable);
		}
	}
	
	public void deleteTermVectorVersionId(TermDocMetadata termDocMeta){
		HTableInterface versionIdTable = SolbaseUtil.getTermVectorVersionIDTable();
		
		Term term = termDocMeta.getTerm();
		byte[] fieldTermKey = SolbaseUtil.generateTermKey(term);
		Delete delete = new Delete(fieldTermKey);
		
		try {
			versionIdTable.delete(delete);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			SolbaseUtil.releaseTable(versionIdTable);
		}
	}
	
	public void deleteTermVector(TermDocMetadata termDocMeta, int startDocId, int endDocId){
		// to update, we should first delete existing term doc meta data.
		// getting terVector and doc tables
		HTableInterface termVectorTable = SolbaseUtil.getTermVectorTable();

		ResultScanner fieldScanner = null;
		
		try {
			byte[] key = termDocMeta.getFieldTermKey();
			int docNumber = termDocMeta.getDocId();
			
			Delete delete = null;

			switch (TermDocMetadataLoader.storageType) {
			case KEY_ONLY: {
				byte[] termBeginKey = Bytes.add(key, SolbaseUtil.delimiter, Bytes.toBytes(docNumber));
				byte[] termEndKey = Bytes.add(key, SolbaseUtil.delimiter, Bytes.toBytes(docNumber+1));

				Scan fieldScan = new Scan(termBeginKey, termEndKey);
				fieldScan.addFamily(SolbaseUtil.termVectorDocColumnFamilyName);
				fieldScanner = termVectorTable.getScanner(fieldScan);

				Result termDoc;

				termDoc = fieldScanner.next();
				fieldScanner.close();
				
				if(termDoc != null){
					delete = new Delete(termDoc.getRow());
				}
			}
				break;
			case WIDE_ROW:
				int chunkId = TermDocMetadataLoader.getChunkId(docNumber);
				delete = new Delete(Bytes.add(key, SolbaseUtil.delimiter, Bytes.toBytes(chunkId)));
				break;
			case NARROW_ROW:
			default: {
				delete = new Delete(Bytes.add(key, SolbaseUtil.delimiter, Bytes.toBytes(docNumber)));
			}
			}
			
			if(delete != null){
				termVectorTable.delete(delete);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(fieldScanner != null){
				fieldScanner.close();
			}
			SolbaseUtil.releaseTable(termVectorTable);
		}
	}
	
	@SuppressWarnings("unchecked")
	public ParsedDoc parseDoc(Document doc, Analyzer analyzer, String indexName, int docNumber, List<String> sortFieldNames) throws CorruptIndexException, IOException {
		// given doc, what are all of terms we indexed
		List<Term> allIndexedTerms = new ArrayList<Term>();
		Map<String, byte[]> fieldCache = new HashMap<String, byte[]>(1024);

		// need to hold onto TermDocMetaData, so it can return this array
		List<TermDocMetadata> metadatas = new ArrayList<TermDocMetadata>();

		byte[] docId = Bytes.toBytes(docNumber);
		int position = 0;

		for (Fieldable field : (List<Fieldable>) doc.getFields()) {
			// Indexed field
			if (field.isIndexed() && field.isTokenized()) {

				TokenStream tokens = field.tokenStreamValue();

				if (tokens == null) {
					tokens = analyzer.tokenStream(field.name(), new StringReader(field.stringValue()));
				}

				// collect term information per field
				Map<Term, Map<ByteBuffer, List<Number>>> allTermInformation = new ConcurrentSkipListMap<Term, Map<ByteBuffer, List<Number>>>();

				int lastOffset = 0;
				if (position > 0) {
					position += analyzer.getPositionIncrementGap(field.name());
				}

				tokens.reset(); // reset the TokenStream to the first token

				// offsets
				OffsetAttribute offsetAttribute = null;
				if (field.isStoreOffsetWithTermVector())
					offsetAttribute = (OffsetAttribute) tokens.addAttribute(OffsetAttribute.class);

				// positions
				PositionIncrementAttribute posIncrAttribute = null;
				if (field.isStorePositionWithTermVector())
					posIncrAttribute = (PositionIncrementAttribute) tokens.addAttribute(PositionIncrementAttribute.class);

				TermAttribute termAttribute = (TermAttribute) tokens.addAttribute(TermAttribute.class);

				// store normalizations of field per term per document
				// rather
				// than per field.
				// this adds more to write but less to read on other side
				Integer tokensInField = new Integer(0);

				while (tokens.incrementToken()) {
					tokensInField++;
					Term term = new Term(field.name(), termAttribute.term());

					allIndexedTerms.add(term);

					// fetch all collected information for this term
					Map<ByteBuffer, List<Number>> termInfo = allTermInformation.get(term);

					if (termInfo == null) {
						termInfo = new ConcurrentSkipListMap<ByteBuffer, List<Number>>();
						allTermInformation.put(term, termInfo);
					}

					// term frequency
					List<Number> termFrequency = termInfo.get(TermDocMetadata.termFrequencyKeyBytes);
					if (termFrequency == null) {
						termFrequency = new ArrayList<Number>();
						termFrequency.add(new Integer(0));
						termInfo.put(TermDocMetadata.termFrequencyKeyBytes, termFrequency);
					}
					// increment
					termFrequency.set(0, termFrequency.get(0).intValue() + 1);

					// position vector
					if (field.isStorePositionWithTermVector()) {
						position += (posIncrAttribute.getPositionIncrement() - 1);

						List<Number> positionVector = termInfo.get(TermDocMetadata.positionVectorKeyBytes);

						if (positionVector == null) {
							positionVector = new ArrayList<Number>();
							termInfo.put(TermDocMetadata.positionVectorKeyBytes, positionVector);
						}

						positionVector.add(++position);
					}

					// term offsets
					if (field.isStoreOffsetWithTermVector()) {

						List<Number> offsetVector = termInfo.get(TermDocMetadata.offsetVectorKeyBytes);
						if (offsetVector == null) {
							offsetVector = new ArrayList<Number>();
							termInfo.put(TermDocMetadata.offsetVectorKeyBytes, offsetVector);
						}

						offsetVector.add(lastOffset + offsetAttribute.startOffset());
						offsetVector.add(lastOffset + offsetAttribute.endOffset());

					}
					
					List<Number> sortValues = new ArrayList<Number>();
					// init sortValues
					for(int i = 0; i < Scorer.numSort; i++){
						sortValues.add(new Integer(-1));
					}
					
					int order = 0;
					
					// extract sort field value and store it in term doc metadata obj
					for(String fieldName: sortFieldNames){
						Fieldable fieldable = doc.getFieldable(fieldName);
						
						if (fieldable instanceof EmbeddedSortField) {
							EmbeddedSortField sortField = (EmbeddedSortField) fieldable;

							int value = -1;
							if (sortField.stringValue() != null) {
								value = Integer.parseInt(sortField.stringValue());
							}
							int sortSlot = sortField.getSortSlot();

							sortValues.set(sortSlot - 1, new Integer(value));
						} else {
							// TODO: this logic is used for real time indexing.
							// hacky. depending on order of sort field names in array
							int value = -1;
							if(fieldable.stringValue() != null){
								value = Integer.parseInt(fieldable.stringValue());
							}
							sortValues.set(order++, new Integer(value));
						}
					}
					termInfo.put(TermDocMetadata.sortFieldKeyBytes, sortValues);
				}

				List<Number> bnorm = null;
				if (!field.getOmitNorms()) {
					bnorm = new ArrayList<Number>();
					float norm = doc.getBoost();
					norm *= field.getBoost();
					norm *= similarity.lengthNorm(field.name(), tokensInField);
					bnorm.add(Similarity.encodeNorm(norm));
				}

				for (Map.Entry<Term, Map<ByteBuffer, List<Number>>> term : allTermInformation.entrySet()) {
					Term tempTerm = term.getKey();

					byte[] fieldTermKeyBytes = SolbaseUtil.generateTermKey(tempTerm);

					// Mix in the norm for this field alongside each term
					// more writes but faster on read side.
					if (!field.getOmitNorms()) {
						term.getValue().put(TermDocMetadata.normsKeyBytes, bnorm);
					}

					TermDocMetadata data = new TermDocMetadata(docNumber, term.getValue(), fieldTermKeyBytes, tempTerm);
					metadatas.add(data);
				}
			}

			// Untokenized fields go in without a termPosition
			if (field.isIndexed() && !field.isTokenized()) {
				Term term = new Term(field.name(), field.stringValue());
				allIndexedTerms.add(term);

				byte[] fieldTermKeyBytes = SolbaseUtil.generateTermKey(term);

				Map<ByteBuffer, List<Number>> termMap = new ConcurrentSkipListMap<ByteBuffer, List<Number>>();
				termMap.put(TermDocMetadata.termFrequencyKeyBytes, Arrays.asList(new Number[] {}));
				termMap.put(TermDocMetadata.positionVectorKeyBytes, Arrays.asList(new Number[] {}));

				TermDocMetadata data = new TermDocMetadata(docNumber, termMap, fieldTermKeyBytes, term);
				metadatas.add(data);
			}

			// Stores each field as a column under this doc key
			if (field.isStored()) {

				byte[] _value = field.isBinary() ? field.getBinaryValue() : Bytes.toBytes(field.stringValue());

				// first byte flags if binary or not
				byte[] value = new byte[_value.length + 1];
				System.arraycopy(_value, 0, value, 0, _value.length);

				value[value.length - 1] = (byte) (field.isBinary() ? Byte.MAX_VALUE : Byte.MIN_VALUE);

				// logic to handle multiple fields w/ same name
				byte[] currentValue = fieldCache.get(field.name());
				if (currentValue == null) {
					fieldCache.put(field.name(), value);
				} else {

					// append new data
					byte[] newValue = new byte[currentValue.length + SolbaseUtil.delimiter.length + value.length - 1];
					System.arraycopy(currentValue, 0, newValue, 0, currentValue.length - 1);
					System.arraycopy(SolbaseUtil.delimiter, 0, newValue, currentValue.length - 1, SolbaseUtil.delimiter.length);
					System.arraycopy(value, 0, newValue, currentValue.length + SolbaseUtil.delimiter.length - 1, value.length);

					fieldCache.put(field.name(), newValue);
				}
			}
		}

		Put documentPut = new Put(SolbaseUtil.randomize(docNumber));

		// Store each field as a column under this docId
		for (Map.Entry<String, byte[]> field : fieldCache.entrySet()) {
			documentPut.add(Bytes.toBytes("field"), Bytes.toBytes(field.getKey()), field.getValue());
		}
		
		// in case of real time update, we need to add back docId field
		if(!documentPut.has(Bytes.toBytes("field"), Bytes.toBytes("docId"))){
			
			byte[] docIdStr = Bytes.toBytes(new Integer(docNumber).toString());
			// first byte flags if binary or not
			byte[] value = new byte[docIdStr.length + 1];
			System.arraycopy(docIdStr, 0, value, 0, docIdStr.length);

			value[value.length - 1] = (byte) (Byte.MIN_VALUE);
			documentPut.add(Bytes.toBytes("field"), Bytes.toBytes("docId"), value);
		}
		
		// Finally, Store meta-data so we can delete this document
		documentPut.add(Bytes.toBytes("allTerms"), Bytes.toBytes("allTerms"), SolbaseUtil.toBytes(allIndexedTerms).array());

		ParsedDoc parsedDoc = new ParsedDoc(metadatas, doc, documentPut, fieldCache.entrySet(), allIndexedTerms);
		return parsedDoc;

	}

	public void updateDocument(Term updateTerm, Document doc, Analyzer analyzer, int docNumber, List<String> sortFieldNames) throws CorruptIndexException, IOException {
		// we treat add/update same
		throw new UnsupportedOperationException();
	}

	public int docCount() {
		throw new RuntimeException("not supported");
	}

	public String getIndexName() {
		return indexName.get();
	}

	public void setIndexName(String indexName) {
		IndexWriter.indexName.set(indexName);
	}

}
