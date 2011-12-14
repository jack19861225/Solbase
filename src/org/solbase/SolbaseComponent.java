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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.document.FieldSelector;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.highlight.SolrHighlighter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexReader;
import org.solbase.lucenehbase.IndexReader;
import org.solbase.lucenehbase.ReaderCache;

public class SolbaseComponent extends SearchComponent {
	private static AtomicBoolean hasSolbaseSchema = new AtomicBoolean(false);
	private static final Logger logger = Logger.getLogger(SolbaseComponent.class);

	public SolbaseComponent() {

	}

	public String getDescription() {
		return "Reopens Lucandra readers";
	}

	public String getSource() {
		return null;
	}

	public String getSourceId() {
		return null;
	}

	public String getVersion() {
		return "1.0";
	}

	public void prepare(ResponseBuilder rb) throws IOException {

		// Only applies to my lucandra index readers
		if (rb.req.getSearcher().getIndexReader().getVersion() != Long.MAX_VALUE)
			return;

		if (!hasSolbaseSchema.get()) {
			hasSolbaseSchema.set(true);
		}

		// If this is a shard request then no need to do anything
		if (rb.req.getParams().getBool(ShardParams.IS_SHARD, false)) {
			String indexName = (String) rb.req.getContext().get("solbase-index");

			if (indexName == null)
				throw new IOException("Missing core name");

			logger.debug(indexName);
			IndexReader reader = (IndexReader) ((SolrIndexReader) rb.req.getSearcher().getIndexReader()).getWrappedReader();
			
			reader.setIndexName(indexName);

			return;
		}

		String indexName = rb.req.getCore().getName();

		if (indexName.equals("")) {
			return; //
		} else {
			logger.debug("core: " + indexName);
		}

		if (rb.shards == null) {
	        // find number of shards
			// this is current max doc id
            // for real time, we'd have to fetch max doc id from table. maybe put some caching around this number
			//int docId = SolbaseUtil.getSequenceId();

            int numShards = SolbaseShardUtil.getNumShard();

            //run local
            if(numShards == 0) {
                IndexReader reader = (IndexReader) ((SolrIndexReader) rb.req.getSearcher().getIndexReader())
                .getWrappedReader();

                String subIndex = indexName;
                reader.setIndexName(subIndex);
                                                                                                                                                             
                return;
            }   
                 
            
            String[] shards = new String[numShards];
            // assign shards
            List<String> hosts = SolbaseShardUtil.getShardHosts();
	        for (int i=0; i<hosts.size(); i++) {
	        	String host = hosts.get(i);
	        	String shard = host + "/solbase/"+indexName+"~" + i;
                if(logger.isDebugEnabled())
                    logger.debug("Adding shard(" + indexName + "): " + shard);
                
	        	shards[i] = shard;
	        }

            // assign to shards
            rb.shards = shards;

			return;

		}
	}

	public void process(ResponseBuilder rb) throws IOException {

		DocList list = rb.getResults().docList;

		DocIterator it = list.iterator();

		List<Integer> docIds = new ArrayList<Integer>(list.size());

		while (it.hasNext())
			docIds.add(it.next());
		
        IndexReader reader = (IndexReader) ((SolrIndexReader) rb.req.getSearcher().getIndexReader())
        .getWrappedReader();
        
	    SolrQueryRequest req = rb.req;

	    SolrParams params = req.getParams();
	    String ids = params.get(ShardParams.IDS);
	    
	    // only set firstPhase threadLocal in case of sharding.
	    // this is to optimize our querying time by not fetching actually doc obj in first phase of sharding
	    // first phase of sharding only tries to fetch docids and scores which are already in tv
	    if(SolbaseShardUtil.getNumShard() != 0){	    
			if (ids != null) {
				IndexReader.firstPhase.set(false);
			} else {
				IndexReader.firstPhase.set(true);
			}
	    } else {
	    	// it's always false in case of stand alone
	    	IndexReader.firstPhase.set(false);
	    }
	    
		logger.debug(reader.getIndexName() + " : Fetching " + docIds.size() + " Docs");

		if (docIds.size() > 0) {

			List<byte[]> fieldFilter = null;
			Set<String> returnFields = rb.rsp.getReturnFields();
			if (returnFields != null) {

				// copy return fields list
				fieldFilter = new ArrayList<byte[]>(returnFields.size());
				for (String field : returnFields) {
					fieldFilter.add(Bytes.toBytes(field));
				}

				// add highlight fields
				SolrHighlighter highligher = rb.req.getCore().getHighlighter();
				if (highligher.isHighlightingEnabled(rb.req.getParams())) {
					for (String field : highligher.getHighlightFields(rb.getQuery(), rb.req, null))
						if (!returnFields.contains(field))
							fieldFilter.add(Bytes.toBytes(field));
				}
				// fetch unique key if one exists.
				SchemaField keyField = rb.req.getSearcher().getSchema().getUniqueKeyField();
				if (null != keyField)
					if (!returnFields.contains(keyField))
						fieldFilter.add(Bytes.toBytes(keyField.getName()));
			}

			FieldSelector selector = new SolbaseFieldSelector(docIds, fieldFilter);

			// This will bulk load these docs
			rb.req.getSearcher().getReader().document(docIds.get(0), selector);

		}

		ReaderCache.flushThreadLocalCaches(reader.getIndexName());
	}

}
