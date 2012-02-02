package org.solbase.cache;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.solr.search.DocList;
import org.apache.solr.search.LRUCache;
import org.apache.solr.search.SolrCacheWithReader;
import org.solbase.lucenehbase.CompactedTermDocMetadataArray;
import org.solbase.lucenehbase.ReaderCache;
import org.solbase.lucenehbase.TermDocMetadataVersionIdentifier;

public class QueryResultCache extends LRUCache implements SolrCacheWithReader, QueryResultCacheMBean{

	private String indexName;
	private int startDocId;
	private int endDocId;
	// 2 hours query result cache expiration; 
	// TODO: making it 2 hours since we have cross shard version update issue
	// if one of shard updates version identifier, replication only picks up version changes, but not actual term vector changes,
	// it will cache stale version of term vector and never expire query result cache
	public static final long QUERY_RESULT_CACHE_TIMEOUT = 1000 * 60 * 120; 
	
	private final static Logger logger = Logger.getLogger(QueryResultCache.class);
	
	public QueryResultCache(){
		super();
		// register mbean here
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
	    ObjectName objName;
		try {
			objName = new ObjectName("org.solbase.cache:type=QueryResultCache"+Math.random());
			
			mbs.registerMBean(this, objName);
		} catch (InstanceAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MBeanRegistrationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotCompliantMBeanException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		} catch (MalformedObjectNameException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NullPointerException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
	}
	@Override
	public void setReader(IndexReader reader) {
		this.indexName = ((org.solbase.lucenehbase.IndexReader)reader).getIndexName();
		this.startDocId = ((org.solbase.lucenehbase.IndexReader)reader).getStartDocId();
		this.endDocId = ((org.solbase.lucenehbase.IndexReader)reader).getEndDocId();
	}

	public Object put(Object key, Object value, Set terms)  {
		try {
			return super.put(key, new DocListCachedWrapper((DocList)value, terms, indexName, startDocId, endDocId));
		} catch (Exception e) {
			return null;
		} 
	}

	public Object get(Object key, Set terms) {
		
		DocListCachedWrapper dlcw = (DocListCachedWrapper)this.get(key);
		if (dlcw == null) {
			// we don't have query result cache given query
			return null;
		}
		
		long dlcwTs = dlcw.getTimestamp();
		long currentTs = System.currentTimeMillis();
		if((currentTs - dlcwTs) > QUERY_RESULT_CACHE_TIMEOUT) {
			// we are blindly expiring query result cache if time has passed
			return null;
		}
		
		for (Term term : (Set<Term>) terms) {
			CachedObjectWrapper<CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier> cow;
			try {
				cow = ReaderCache.getTermDocsMetadata(term, indexName, startDocId, endDocId);
			} catch (Exception e) {
				return null;
			}
			Long cachedVersion = dlcw.getVersionId(term);
			if (cachedVersion == null || cow.getVersionIdentifier().getVersionIdentifier() != cachedVersion) {
				return null;
			}
		}
		
		return dlcw.getDocList();
	}
	
	@Override 
	public Object put(Object key, Object value) {
		throw new UnsupportedOperationException();
	}

	public void acquireLock(Set terms){
		for (Term term : (Set<Term>) terms) {
			CachedObjectWrapper<CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier> cow;
			CompactedTermDocMetadataArray ctdm = null;
			try {
				cow = ReaderCache.getTermDocsMetadata(term, indexName, startDocId, endDocId);
				ctdm = cow.getValue();
				
				if(ctdm.readWriteLock.getReadLockCount() > 10){
					logger.debug(term.toString() + " has this many read locks currently: " + ctdm.readWriteLock.getReadLockCount());
				}
				if(ctdm != null){
					ctdm.readWriteLock.readLock().lock();
				}
			} catch (Exception e) {
			}
		}	
	}
	
	public void releaseLock(Set terms){
		for (Term term : (Set<Term>) terms) {
			CachedObjectWrapper<CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier> cow;
			CompactedTermDocMetadataArray ctdm = null;
			try {
				cow = ReaderCache.getTermDocsMetadata(term, indexName, startDocId, endDocId);
				ctdm = cow.getValue();
				
				if(ctdm != null){
					ctdm.readWriteLock.readLock().unlock();
				}
			} catch (Exception e) {
			}
		}	
	}
}
