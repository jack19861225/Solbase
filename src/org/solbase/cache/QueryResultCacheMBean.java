package org.solbase.cache;

import org.apache.solr.common.util.NamedList;

public interface QueryResultCacheMBean {
	  public NamedList getStatistics();
	  public long getLookups();
	  public long getHits();
	  public long getInserts();
	  public long getEvictions();
	  public void freezeCacheSize();
	  public void resetCacheLimit(int limit);
}
