/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.core;

import java.util.List;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.solbase.SolbaseShardUtil;

public class SolbaseQuerySenderListener extends AbstractSolrEventListener {
	public SolbaseQuerySenderListener(SolrCore core) {
		super(core);
	}

	@Override
	public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
		// SolbaseCoreContainer creates empty core for reading schema, etc. so
		// don't warm up this core. only warm up shard cores
		if (!core.getName().equals("") && core.getName().indexOf("~") > 0) {
			final SolrIndexSearcher searcher = newSearcher;
			log.info("QuerySenderListener sending requests to " + core.getName());

			for (NamedList nlst : (List<NamedList>) args.get("queries")) {
				try {
					SolrQueryResponse rsp = new SolrQueryResponse();

					LocalSolrQueryRequest req = new LocalSolrQueryRequest(core, nlst) {
						@Override
						public SolrIndexSearcher getSearcher() {
							return searcher;
						}

						@Override
						public void close() {
						}
					};

					req.getContext().put("solbase-index", core.getName());
					req.getContext().put("webapp", "/solbase");
					req.getContext().put("path", "/select");

					core.execute(core.getRequestHandler(req.getParams().get(core.getName())), req, rsp);

					req.close();

				} catch (Exception e) {
					// do nothing... we want to continue with the other
					// requests.
					// the failure should have already been logged.
				}
			}
			log.info("QuerySenderListener done.");
		}
	}

}
