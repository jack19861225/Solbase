package org.solbase.cache;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.MapMaker;

public class SoftReferenceCache<K, V, Z> extends VersionedCache<K, V, Z> {

	

	
	private Map<K, CachedObjectWrapper<V, Z>> cache = new MapMaker().softValues().makeMap();

	@Override
	protected CachedObjectWrapper<V, Z> getInternal(K key) throws IOException {
		return cache.get(key);
	}

	@Override
	public void put(K key, CachedObjectWrapper<V, Z> aValue) throws IOException {
		cache.put(key, aValue);
	}

	@Override
	public void clear() throws IOException {
		cache.clear();
	}

}
