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
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.index.Term;
import org.apache.solr.schema.IndexSchema;
import org.solbase.cache.CachedObjectWrapper;
import org.solbase.cache.LayeredCache;
import org.solbase.cache.SolbaseLRUCache;
import org.solbase.cache.ThreadLocalCache;
import org.solbase.cache.VersionedCache;
import org.solbase.indexer.ParsedDoc;

public class ReaderCache {
	
	public static final long DOCUMENT_CACHE_TIMEOUT = 1000 * 60 * 120; //2 hours;
	// 2 hours; TODO: making it 2 hours since we have cross shard version expiring issue
	// if one of shard updates version identifier for given term, other shards do not have to load, but since version identifier changed,
	// it will try to load same data again from hbase.
	public static final long TERM_DOC_METADATA_CACHE_TIMEOUT = 1000 * 60 * 120; 
	
	private static class Caches {
		ThreadLocalCache<Integer, Document, Long> docThreadLocalCache;
		ThreadLocalCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier> termDocMetadatasLocalCache;
		LayeredCache<Integer, Document, Long, ParsedDoc> luceneDocument;
		LayeredCache<String, Document, Long, Document> shardDocument;
		LayeredCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier, TermDocMetadata> termDocMetadatas;
	}
	
	public static IndexSchema schema;
	
	private static TreeMap<String, Caches> caches = new TreeMap<String, Caches>();
	
	public static final Object fieldCacheKey = UUID.randomUUID();
	
	public static CachedObjectWrapper<Document, Long> getDocument(String docIdKey, FieldSelector fieldSelector, String indexName, int start, int end) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		return getCache(indexName).shardDocument.getCachedObject(docIdKey, new ShardDocumentLoader(fieldSelector), indexName, start, end);
	}

	public static CachedObjectWrapper<Document, Long> getDocument(Integer docId, FieldSelector fieldSelector, String indexName, int start, int end) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		return getCache(indexName).luceneDocument.getCachedObject(docId, new DocumentLoader(fieldSelector, schema), indexName, start, end);
	}
	
	public static CachedObjectWrapper<Document, Long> getDocument(Integer docId, FieldSelector fieldSelector, String indexName, int start, int end, HTableInterface htable) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		return getCache(indexName).luceneDocument.getCachedObject(docId, new DocumentLoader(fieldSelector, schema, htable), indexName, start, end);
	}

	public static CachedObjectWrapper<CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier> getTermDocsMetadata(Term term, String indexName, int start, int end) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		return getCache(indexName).termDocMetadatas.getCachedObject(term, indexName, start, end);
	}
	/*
	public static CachedObjectWrapper<CompactedTermDocMetadataArray, Long> getTermDocsMetadataFromCacheOnly(Term term, String indexName) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		return getCache(indexName).termDocMetadatas.getCachedObjectFromCacheOnly(term, indexName);
	}
	*/
	
	public static void updateTermDocsMetadata(Term term, TermDocMetadata modificationData, String indexName, IndexWriter writer, LayeredCache.ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		getCache(indexName).termDocMetadatas.updateCachedObject(term, modificationData, indexName, writer, modType, updateStore, startDocId, endDocId);
	}
	
	public static void updateDocument(String docId, Document document, String indexName, IndexWriter writer, LayeredCache.ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		getCache(indexName).shardDocument.updateCachedObject(docId, document, new ShardDocumentLoader(null), indexName, writer, modType, updateStore, startDocId, endDocId);
	}

	public static void updateDocument(Integer docId, ParsedDoc document, String indexName, IndexWriter writer, LayeredCache.ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		getCache(indexName).luceneDocument.updateCachedObject(docId, document, new DocumentLoader(null, ReaderCache.schema), indexName, writer, modType, updateStore, startDocId, endDocId);
	}

	public static void flushThreadLocalCaches(String indexName) throws IOException {
		getCache(indexName).docThreadLocalCache.clear();
		getCache(indexName).termDocMetadatasLocalCache.clear();
	}
	
	// note - 200k docs and 500k tv sum'd up to 15~17G total heap space
	private static synchronized Caches getCache(String indexName) {
		Caches cacheGroup = caches.get(indexName);
		
		if (cacheGroup == null ) {
			cacheGroup = new Caches();
			
			// Document caching
			ArrayList<VersionedCache<Integer, Document, Long>> docCaches = new ArrayList<VersionedCache<Integer, Document, Long>>();
			cacheGroup.docThreadLocalCache = new ThreadLocalCache<Integer, Document, Long>();
			docCaches.add(cacheGroup.docThreadLocalCache);
			docCaches.add(new SolbaseLRUCache<Integer, Document, Long>(500, "Document~"+indexName)); // 1024 * 500 = 500k
			//docCaches.add(new SoftReferenceCache<Integer, Document, Long>());
			//docCaches.add(new MemcacheCache<Integer, Document, Long>());
			cacheGroup.luceneDocument = new LayeredCache<Integer, Document, Long, ParsedDoc>(DOCUMENT_CACHE_TIMEOUT, docCaches);

			// Solr shard doc caching
			// maybe I only need soft reference memory cache instead of memcache for solr doc
			//ArrayList<VersionedCache<String, Document, Long>> solrDocCaches = new ArrayList<VersionedCache<String, Document, Long>>();
			//solrDocCaches.add(new SolbaseLRUCache<String, Document, Long>(500));
			//solrDocCaches.add(new SoftReferenceCache<String, Document, Long>());
			//solrDocCaches.add(new MemcacheCache<String, Document, Long>());
			//cacheGroup.shardDocument = new LayeredCache<String, Document, Long>(CACHE_TIMEOUT, solrDocCaches);
			
			// Term Vector Caching
			ArrayList<VersionedCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier>> termDocMetadataCaches = new ArrayList<VersionedCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier>>();
			cacheGroup.termDocMetadatasLocalCache = new ThreadLocalCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier>();
			termDocMetadataCaches.add(cacheGroup.termDocMetadatasLocalCache);
			termDocMetadataCaches.add(new SolbaseLRUCache<Term,CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier>(250, "TV~"+indexName)); // 1024 * 250 = 250k 
			//termDocMetadataCaches.add(new SoftReferenceCache<Term,SerializableTermDocMetadataArray, Long>());
			//termDocMetadataCaches.add(new MemcacheCache<Term,SerializableTermDocMetadataArray, Long>());
			cacheGroup.termDocMetadatas = new LayeredCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier, TermDocMetadata>(new TermDocMetadataLoader(), TERM_DOC_METADATA_CACHE_TIMEOUT, termDocMetadataCaches);
			
			caches.put(indexName, cacheGroup);
		}
		
		return cacheGroup;
	}

}
