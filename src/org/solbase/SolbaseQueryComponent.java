package org.solbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryResponse;
import org.apache.solr.schema.EmbeddedSortField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexReader;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;

public class SolbaseQueryComponent extends QueryComponent {

	/**
	 * Actually run the query
	 */
	@Override
	public void process(ResponseBuilder rb) throws IOException {

		SolrQueryRequest req = rb.req;
		SolrQueryResponse rsp = rb.rsp;
		SolrParams params = req.getParams();
		if (!params.getBool(COMPONENT_NAME, true)) {
			return;
		}
		SolrIndexSearcher searcher = req.getSearcher();

		if (rb.getQueryCommand().getOffset() < 0) {
			throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'start' parameter cannot be negative");
		}

		// -1 as flag if not set.
		long timeAllowed = (long) params.getInt(CommonParams.TIME_ALLOWED, -1);

		// Optional: This could also be implemented by the top-level searcher
		// sending
		// a filter that lists the ids... that would be transparent to
		// the request handler, but would be more expensive (and would preserve
		// score
		// too if desired).
		String ids = params.get(ShardParams.IDS);
		if (ids != null) {
			List<String> idArr = StrUtils.splitSmart(ids, ",", true);
			int[] luceneIds = new int[idArr.size()];
			int docs = 0;
			for (int i = 0; i < idArr.size(); i++) {
				luceneIds[docs++] = Integer.parseInt(idArr.get(i));
			}

			// we are indexing docId as solr uniq_id. by doing this, we are
			// bound to INTEGER.MAX_VALUE ~= 2 billion
			// docs is number of docs
			DocListAndSet res = new DocListAndSet();
			res.docList = new DocSlice(0, docs, luceneIds, null, docs, 0, null);
			if (rb.isNeedDocSet()) {
				List<Query> queries = new ArrayList<Query>();
				queries.add(rb.getQuery());
				List<Query> filters = rb.getFilters();
				if (filters != null)
					queries.addAll(filters);
				res.docSet = searcher.getDocSet(queries);
			}
			rb.setResults(res);
			rsp.add("response", rb.getResults().docList);
			return;
		}

		SolrIndexSearcher.QueryCommand cmd = rb.getQueryCommand();
		cmd.setTimeAllowed(timeAllowed);
		SolrIndexSearcher.QueryResult result = new SolrIndexSearcher.QueryResult();
		
		searcher.search(result, cmd);
		
		rb.setResult(result);

		rsp.add("response", rb.getResults().docList);
		rsp.getToLog().add("hits", rb.getResults().docList.matches());

		// The query cache doesn't currently store sort field values, and
		// SolrIndexSearcher doesn't
		// currently have an option to return sort field values. Because of
		// this, we
		// take the documents given and re-derive the sort values.
		boolean fsv = req.getParams().getBool(ResponseBuilder.FIELD_SORT_VALUES, false);
		if (fsv) {
			Sort sort = rb.getSortSpec().getSort();
			SortField[] sortFields = sort == null ? new SortField[] { SortField.FIELD_SCORE } : sort.getSort();
			NamedList sortVals = new NamedList(); // order is important for the
													// sort fields
			Field field = new Field("dummy", "", Field.Store.YES, Field.Index.NO); // a
																					// dummy
																					// Field

			SolrIndexReader reader = searcher.getReader();
			SolrIndexReader[] readers = reader.getLeafReaders();
			SolrIndexReader subReader = reader;
			if (readers.length == 1) {
				// if there is a single segment, use that subReader and avoid
				// looking up each time
				subReader = readers[0];
				readers = null;
			}
			int[] offsets = reader.getLeafOffsets();

			
			//TODO: need to fetch sort value from collector instead of re-derive lookup from id
			for (SortField sortField : sortFields) {
				int type = sortField.getType();
				if (type == SortField.SCORE || type == SortField.DOC)
					continue;

				FieldComparator comparator = null;
				FieldComparator comparators[] = (readers == null) ? null : new FieldComparator[readers.length];

				String fieldname = sortField.getField();
				FieldType ft = fieldname == null ? null : req.getSchema().getFieldTypeNoEx(fieldname);

				DocSlice docList = (DocSlice)rb.getResults().docList;
				
				ArrayList<Object> vals = new ArrayList<Object>(docList.size());
				for (int i = docList.offset; i < docList.len; i++) {
					vals.add(new Integer(docList.sorts[i][((EmbeddedSortField)sortField).getFieldNumber() - 1]));
				}

				sortVals.add(fieldname, vals);
			}

			rsp.add("sort_values", sortVals);
		}

		// pre-fetch returned documents
		if (!req.getParams().getBool(ShardParams.IS_SHARD, false) && rb.getResults().docList != null && rb.getResults().docList.size() <= 50) {
			// TODO: this may depend on the highlighter component (or other
			// components?)
			SolrPluginUtils.optimizePreFetchDocs(rb.getResults().docList, rb.getQuery(), req, rsp);
		}
	}
}
