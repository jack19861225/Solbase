package org.solbase.cache;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.ResourceBundle;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.LRUCache;
import org.apache.solr.search.SolrCache.State;

public class SolbaseLRUCache<K, V, Z> extends VersionedCache<K, V, Z> implements SolbaseLRUCacheMBean{

	LRUCache cache;
	private int size;
	
	public SolbaseLRUCache(int cacheSize, String name) {
		String size = System.getProperty("solbase.lrucache.size");

		try {
			if (size == null && ResourceBundle.getBundle("solbase") != null) {
				size = ResourceBundle.getBundle("solbase").getString("lrucache.size");
			}
		} catch (Exception ex) {

		}
		
		if (size == null) {
			size = new Integer(1024 * cacheSize).toString();
		}
		
		this.size = new Integer(size);

		HashMap<String, String> argMap = new HashMap<String, String>();
		argMap.put("size", size);
		argMap.put("name", "solbaseCache");
		
		cache = new LRUCache();
		cache.init(argMap, null, null);
		cache.setState(State.LIVE);
		// register mbean here
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer(); 
	    ObjectName objName;
		try {
			objName = new ObjectName("org.solbase.cache:type=SolbaseLRUCache-"+name);
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

	@SuppressWarnings("unchecked")
	@Override
	protected CachedObjectWrapper<V, Z> getInternal(K key) throws IOException {
		return (CachedObjectWrapper<V, Z>) cache.get(key);
	}

	@Override
	public void put(K key, CachedObjectWrapper<V, Z> aValue) throws IOException {
		cache.put(key, aValue);
	}

	@Override
	public void clear() throws IOException {
		cache.clear();
	}
	@Override
	public boolean isCacheFull() {
		return this.size <= cache.size();
	}

	@Override
	public NamedList getStatistics() {
		return this.cache.getStatistics();
	}

	@Override
	public long getLookups() {
		return this.cache.getLookups();
	}

	@Override
	public long getHits() {
		return this.cache.getHits();
	}

	@Override
	public long getInserts() {
		return this.cache.getInserts();
	}

	@Override
	public long getEvictions() {
		return this.cache.getEvictions();
	}

	@Override
	public void freezeCacheSize() {
		this.cache.freezeCacheSize();
	}

	@Override
	public void resetCacheLimit(int limit) {
		this.cache.resetCacheLimit(limit);
	}

}
