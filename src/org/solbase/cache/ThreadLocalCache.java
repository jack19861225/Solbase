package org.solbase.cache;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ThreadLocalCache<K, V, Z> extends VersionedCache<K, V, Z> {


	
	private ThreadLocal<Map<K, CachedObjectWrapper<V, Z>>> threadLevelCache = new ThreadLocal<Map<K, CachedObjectWrapper<V, Z>>>() {
		@Override
		protected Map<K, CachedObjectWrapper<V, Z>> initialValue() {
			return new HashMap<K, CachedObjectWrapper<V, Z>>();
		}
	};
	
	@Override
	protected CachedObjectWrapper<V, Z> getInternal(K key) throws IOException {
		return threadLevelCache.get().get(key);
	}

	@Override
	public void put(K key, CachedObjectWrapper<V, Z> aValue) throws IOException {
		threadLevelCache.get().put(key, aValue);
	}

	@Override
	public void clear() throws IOException {
		threadLevelCache.get().clear();
	}

}
