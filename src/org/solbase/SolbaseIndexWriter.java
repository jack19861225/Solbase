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
package org.solbase;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.naming.OperationNotSupportedException;

import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.MergeIndexesCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.solbase.cache.CachedObjectWrapper;
import org.solbase.cache.LayeredCache;
import org.solbase.indexer.ParsedDoc;
import org.solbase.indexer.SolbaseIndexUtil;
import org.solbase.lucenehbase.ReaderCache;
import org.solbase.lucenehbase.TermDocMetadata;

public class SolbaseIndexWriter extends UpdateHandler {
	// To manage cached reads
	private static final LinkedBlockingQueue<String> flushQueue = new LinkedBlockingQueue<String>();
	private final ExecutorService flushMonitor = Executors.newSingleThreadExecutor();

	private final org.solbase.lucenehbase.IndexWriter writer;
	private final static Logger logger = Logger.getLogger(SolbaseIndexWriter.class);

	private static SolbaseIndexUtil indexUtil;

	// stats
	AtomicLong addCommands = new AtomicLong();
	AtomicLong addCommandsCumulative = new AtomicLong();
	AtomicLong deleteByIdCommands = new AtomicLong();
	AtomicLong deleteByIdCommandsCumulative = new AtomicLong();
	AtomicLong deleteByQueryCommands = new AtomicLong();
	AtomicLong deleteByQueryCommandsCumulative = new AtomicLong();
	AtomicLong expungeDeleteCommands = new AtomicLong();
	AtomicLong mergeIndexesCommands = new AtomicLong();
	AtomicLong commitCommands = new AtomicLong();
	AtomicLong optimizeCommands = new AtomicLong();
	AtomicLong rollbackCommands = new AtomicLong();
	AtomicLong numDocsPending = new AtomicLong();
	AtomicLong numErrors = new AtomicLong();
	AtomicLong numErrorsCumulative = new AtomicLong();

	public SolbaseIndexWriter(SolrCore core) {
		super(core);

		// set SolbaseIndexUtil
		if(ResourceBundle.getBundle("solbase") != null){
			String className = ResourceBundle.getBundle("solbase").getString("class.solbaseIndexUtil");
			if(className != null){
				try {
					indexUtil = (SolbaseIndexUtil) Class.forName(className).newInstance();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		try {
			writer = new org.solbase.lucenehbase.IndexWriter();

			flushMonitor.execute(new Runnable() {

				public void run() {
					Map<String, Long> lastCoreFlush = new HashMap<String, Long>();

					while (true) {
						try {
							String core = flushQueue.take();

							Long lastFlush = lastCoreFlush.get(core);
							if (lastFlush == null || lastFlush <= (System.currentTimeMillis() - SolbaseUtil.cacheInvalidationInterval)) {
								flush(core);
								lastCoreFlush.put(core, System.currentTimeMillis());
								logger.info("Flushed cache: " + core);
							}
						} catch (InterruptedException e) {
							continue;
						} catch (IOException e) {
							logger.error(e);
						}
					}
				}

				private void flush(String core) throws IOException {
					HTableInterface table = SolbaseUtil.getSchemaInfoTable();
					try {
						Put schemaPut = new Put(Bytes.toBytes("cache"));

						schemaPut.add(Bytes.toBytes("info"), Bytes.toBytes("schema"), Bytes.toBytes(""));

						table.put(schemaPut);
					} finally {
						SolbaseUtil.releaseTable(table);
					}
				}

			});

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void setIndexUtil(SolbaseIndexUtil util) {
		indexUtil = util;
	}

	public int addDoc(AddUpdateCommand cmd) throws IOException {	
		addCommands.incrementAndGet();
		addCommandsCumulative.incrementAndGet();
		int rc = -1;

		// no duplicates allowed
		SchemaField uniqueField = core.getSchema().getUniqueKeyField();

		if (uniqueField == null)
			throw new IOException("Solbase requires a unique field");

		// if there is no ID field, use allowDups
		if (idField == null) {
			throw new IOException("Solbase requires a unique field");
		}

		try {
			String indexName = core.getName();
			writer.setIndexName(indexName);

			Document doc = cmd.getLuceneDocument(schema);
			
			String idFieldName = idTerm.field();
			
			// solbase specific fields. should remove it after using
			boolean updateStore = false;
			String updateVal = doc.get("updateStore");
			if(updateVal != null){
				// updating hbase after cache is updated
				updateStore = true;
			}
			
			int docNumber = Integer.parseInt(doc.get(idFieldName));
			
			// if edit field is present, it's for modification instead of blind add
			String editVal = doc.get("edit");
			
			// we don't need following fields. only used for update api
			doc.removeField("docId");
			doc.removeField("edit");
			doc.removeField("updateStore");
		
			// set indexutil to writer
			writer.setIndexUtil(indexUtil);
			
			String globaId = doc.getField("global_uniq_id").stringValue();
			int shardNum = SolbaseShardUtil.getShardNum(indexName);
			int startDocId = SolbaseShardUtil.getStartDocId(shardNum);
			int endDocId = SolbaseShardUtil.getEndDocId(shardNum);
			
			if(editVal != null){
				logger.info("updating doc: " + docNumber);
				if(editDoc(doc, indexName, docNumber, updateStore)){
					rc = 1;
				}
			} else {
				try {
					logger.info("adding doc: " + docNumber);
					
					ParsedDoc parsedDoc = writer.parseDoc(doc, schema.getAnalyzer(), indexName, docNumber, indexUtil.getSortFieldNames());
					List<TermDocMetadata> termDocMetas = parsedDoc.getTermDocMetadatas();
					// TODO: possible problem
					// doc is not in cache, cluster isn't responsible for update store
					// doc never gets updated in hbase, nor cache
					// for loop below will update tv with this new doc.
					// when searched, it will throw null point exception on this doc
					// therefore, update store first if adding doc (replication can still cause this issue if back'd up)
					ReaderCache.updateDocument(docNumber, parsedDoc, indexName, writer, LayeredCache.ModificationType.ADD, updateStore, startDocId, endDocId);

					for (TermDocMetadata termDocMeta : termDocMetas) {
						ReaderCache.updateTermDocsMetadata(termDocMeta.getTerm(), termDocMeta, indexName, writer, LayeredCache.ModificationType.ADD, updateStore, startDocId, endDocId);
					}

					rc = 1;
					logger.info("adding doc: " + docNumber);
					
				} catch (NumberFormatException e) {
					logger.info("adding doc failed: " + docNumber);
					logger.info(e.toString());
				} catch (InterruptedException e) {
					logger.info("adding doc failed: " + docNumber);
					logger.info(e.toString());
				} catch (MemcachedException e) {
					logger.info("adding doc failed: " + docNumber);
					logger.info(e.toString());
				} catch (TimeoutException e) {
					logger.info("adding doc failed: " + docNumber);
					logger.info(e.toString());
				}
			}
		} finally {
			if (rc != 1) {
				numErrors.incrementAndGet();
				numErrorsCumulative.incrementAndGet();
			}
		}
	
		return rc;

	}

	/**
	 * Doing edit logic here. instead of blindingly inserting, we need to compare new doc with old doc and do appropriate modification
	 * to tv and doc
	 * @param newDoc
	 * @param indexName
	 * @return
	 */
	public boolean editDoc(Document newDoc, String indexName, int docNumber, boolean updateStore){
		
		try {
			CachedObjectWrapper<Document, Long> cachedObj = ReaderCache.getDocument(docNumber, null, indexName, 0, 0);
			if(cachedObj == null || cachedObj.getValue() == null) {
				// document doesn't exist, so let's just bail out here
				return true;
			}
			
			ParsedDoc parsedDoc = new ParsedDoc(newDoc);
			parsedDoc.setIndexName(indexName);
			parsedDoc.setIndexUtil(indexUtil);
			parsedDoc.setIndexWriter(writer);
			parsedDoc.setUpdateStore(updateStore);
			
			int shardNum = SolbaseShardUtil.getShardNum(indexName);
			int startDocId = SolbaseShardUtil.getStartDocId(shardNum);
			int endDocId = SolbaseShardUtil.getEndDocId(shardNum);
			
			ReaderCache.updateDocument(docNumber, parsedDoc, indexName, writer, LayeredCache.ModificationType.UPDATE, updateStore, startDocId, endDocId);
				
			return true;
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
		
		return false;
		
		
		// need to compare old and new doc here
	}
	
	public void close() throws IOException {
		// hehe
	}

	// TODO: flush all sub-index caches?
	public void commit(CommitUpdateCommand cmd) throws IOException {

	}

	public void delete(DeleteUpdateCommand cmd) throws IOException {
		deleteByIdCommands.incrementAndGet();
		deleteByIdCommandsCumulative.incrementAndGet();

		if (!cmd.fromPending && !cmd.fromCommitted) {
			numErrors.incrementAndGet();
			numErrorsCumulative.incrementAndGet();
			throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "meaningless command: " + cmd);
		}
		if (!cmd.fromPending || !cmd.fromCommitted) {
			numErrors.incrementAndGet();
			numErrorsCumulative.incrementAndGet();
			throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "operation not supported" + cmd);
		}

		// Delete all terms/fields/etc
		String indexName = core.getName();
		writer.setIndexName(indexName);
		writer.setIndexUtil(indexUtil);
		
		int docId = Integer.parseInt(cmd.id);
		
		logger.info("deleting doc: " + docId);
		
		try {
			CachedObjectWrapper<Document, Long> wrapper = ReaderCache.getDocument(docId, null, indexName, 0, 0);
			
			boolean updateStore = cmd.getUpdateStore();
			
			ParsedDoc parsedDoc = new ParsedDoc();
			parsedDoc.setIndexName(indexName);
			parsedDoc.setIndexUtil(indexUtil);
			parsedDoc.setIndexWriter(writer);
			parsedDoc.setUpdateStore(updateStore);

			int shardNum = SolbaseShardUtil.getShardNum(indexName);
			int startDocId = SolbaseShardUtil.getStartDocId(shardNum);
			int endDocId = SolbaseShardUtil.getEndDocId(shardNum);
			
			ReaderCache.updateDocument(docId, parsedDoc, indexName, writer, LayeredCache.ModificationType.DELETE, updateStore, startDocId, endDocId);
			
			// let's clean up Docs and DocKeyIdMap tables after deleting doc from term vector
			if(wrapper != null && updateStore){
				Document doc = wrapper.getValue();
				String globalUniqId = doc.get("global_uniq_id");
				Put documentPut = new Put(SolbaseUtil.randomize(docId));
				Put mappingPut = new Put(Bytes.toBytes(globalUniqId));
				
				mappingPut.add(SolbaseUtil.docIdColumnFamilyName, SolbaseUtil.tombstonedColumnFamilyQualifierBytes, Bytes.toBytes(1));
				
				writer.deleteDocument(documentPut);
				writer.updateDocKeyIdMap(mappingPut);
			}
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
	}

	public void deleteByQuery(DeleteUpdateCommand cmd) throws IOException {
		try {
			throw new OperationNotSupportedException();
		} catch (OperationNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int mergeIndexes(MergeIndexesCommand cmd) throws IOException {
		return 0;
	}

	public void rollback(RollbackUpdateCommand cmd) throws IOException {
		// TODO Auto-generated method stub

	}

	public Category getCategory() {
		return Category.UPDATEHANDLER;
	}

	public String getDescription() {
		return "Update handler for Solbase";
	}

	public URL[] getDocs() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getName() {
		return SolbaseIndexWriter.class.getName();
	}

	public String getSource() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getSourceId() {
		// TODO Auto-generated method stub
		return null;
	}

	public NamedList<Long> getStatistics() {
		NamedList<Long> lst = new SimpleOrderedMap<Long>();

		lst.add("rollbacks", rollbackCommands.get());
		lst.add("adds", addCommands.get());
		lst.add("deletesById", deleteByIdCommands.get());
		lst.add("deletesByQuery", deleteByQueryCommands.get());
		lst.add("errors", numErrors.get());
		lst.add("cumulative_adds", addCommandsCumulative.get());
		lst.add("cumulative_deletesById", deleteByIdCommandsCumulative.get());
		lst.add("cumulative_deletesByQuery", deleteByQueryCommandsCumulative.get());
		lst.add("cumulative_errors", numErrorsCumulative.get());
		return lst;
	}

	public String getVersion() {
		return core.getVersion();
	}

	@SuppressWarnings("unused")
	private void clearCache(String core) {
		SolbaseIndexWriter.flushQueue.add(core);
	}

	
	public static void main(String[] args){
		try {
			@SuppressWarnings("deprecation")
			HBaseConfiguration conf = new HBaseConfiguration();
			//conf.set("hbase.zookeeper.quorum", "den2zksb001");
			conf.set("hbase.zookeeper.quorum", "den3dhdptk01.int.photobucket.com");
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			conf.setInt("hbase.client.retries.number", 7);
			conf.setInt("ipc.client.connect.max.retries", 3);

			
			
			HTablePool hTablePool = new HTablePool(conf, 10);
			
			HTableInterface seq = hTablePool.getTable("DocKeyIdMap");			

			String globalId = "0089210673:0000540572:309AB023-orig.jpg";
			Get get = new Get(Bytes.toBytes(globalId));
			
			Result result = seq.get(get);
			
			byte[] docId = result.getValue(Bytes.toBytes("docId"), Bytes.toBytes(""));

			int docNumber = 384900472;
			
			SolrInputDocument doc = new SolrInputDocument();			
			if(docId != null) {
				// we've indexed this doc, so it is edit
				System.out.println(Bytes.toInt(docId));
				docNumber = Bytes.toInt(docId);
				doc.addField("edit", true);
			}
			
			// using seperate connector to leverage different http thread pool for updates
			CommonsHttpSolrServer solbaseServer = new CommonsHttpSolrServer("http://localhost:8080/solbase/pbimages~1");

			doc.addField("docId", docNumber);
			doc.addField("global_uniq_id", globalId);
			doc.addField("title", "tom");
			doc.addField("description", "Uploaded with Snapbucket");
			doc.addField("tags", "Snapbucket");
			doc.addField("path", "/albums/tt262/koh_tester/309AB021-orig.jpg");
			doc.addField("subdomain", "i618");
			doc.addField("lastModified", new Integer(SolbaseUtil.getEpochSinceSolbase(System.currentTimeMillis() / 60000)).toString());
			doc.addField("media_type", new Integer(1).toString());
			doc.addField("total_view_count", new Long(10).toString());
			doc.addField("sevendays_view_count", new Integer(5).toString());
			doc.addField("total_likes_count", new Long(5).toString());
			doc.addField("sevendays_likes_count", new Integer(1).toString());
			doc.addField("total_comments_count", new Long(5).toString());
			doc.addField("sevendays_comments_count", new Integer(1).toString());
			doc.addField("contents", "audi tom solbase Uploaded with Snapbucket ");
			// whether we want to store to hbase or not
			doc.addField("updateStore", true);
			
			solbaseServer.add(doc);

			// for delete only
			//List<String> ids = new ArrayList<String>();
			//ids.add(docNumber + "");			
			//solbaseServer.deleteById(ids, true);
		} catch (MalformedURLException e) {

		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
