package org.solbase.cache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.lucene.index.Term;
import org.apache.solr.search.DocList;
import org.solbase.lucenehbase.CompactedTermDocMetadataArray;
import org.solbase.lucenehbase.ReaderCache;
import org.solbase.lucenehbase.TermDocMetadataVersionIdentifier;

public class DocListCachedWrapper {
	
	private DocList docList;
	private HashMap<Term, Long> termMap = new HashMap<Term, Long>();
	
	
	public DocListCachedWrapper(DocList docList, Set<Term> terms, String indexName, int startDocId, int endDocId) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		this.docList = docList;
		for (Term term : terms) {
			CachedObjectWrapper<CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier> cow = ReaderCache.getTermDocsMetadata(term, indexName, startDocId, endDocId);
			termMap.put(term, cow == null ? 0 :cow.getVersionIdentifier().getVersionIdentifier());
		}
	}
	
	
	public DocList getDocList() {
		return docList;
	}
	
	public Long getVersionId(Term term) {
		return termMap.get(term);
	}

}
