package org.solbase.cache;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import net.rubyeye.xmemcached.exception.MemcachedException;

import org.apache.log4j.Logger;
import org.solbase.lucenehbase.IndexWriter;

public class LayeredCache<K extends Serializable & Comparable<?>, V extends Serializable, Z extends Serializable, M extends Serializable> {

	private final static Logger logger = Logger.getLogger(LayeredCache.class);
	private static final int BACKGROUND_THREAD_POOL_SIZE = 50;

	public static enum ModificationType {
		UPDATE, ADD, DELETE
	}

	private CachedObjectLoader<K, V, Z, M> _loader = null;
	private Long cacheTimeout;
	private List<VersionedCache<K, V, Z>> caches;

	private ThreadPoolExecutor executor = new ThreadPoolExecutor(BACKGROUND_THREAD_POOL_SIZE, BACKGROUND_THREAD_POOL_SIZE, 5, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(10));

	private class LockedResult {
		ReentrantLock lock = new ReentrantLock();
		CachedObjectWrapper<V, Z> result = null;
		int refCount = 0;
	}

	private HashMap<K, LockedResult> currentlyAccesingMap = new HashMap<K, LockedResult>();

	public LayeredCache(Long cacheTimeout, List<VersionedCache<K, V, Z>> caches) {
		this.caches = caches;
		for (VersionedCache<K, V, Z> cache : caches) {
			cache.setTimeout(cacheTimeout);
		}
		this.cacheTimeout = cacheTimeout;
	}

	public LayeredCache(CachedObjectLoader<K, V, Z, M> loader, Long cacheTimeout, List<VersionedCache<K, V, Z>> caches) {
		this(cacheTimeout, caches);
		this._loader = loader;
	}

	public CachedObjectWrapper<V, Z> getCachedObject(K key, String indexName, int start, int end) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		return getCachedObject(key, _loader, indexName, start, end);
	}

	/*
	public CachedObjectWrapper<V, Z> getCachedObjectFromCacheOnly(K key, String indexName) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		return getCachedObjectFromCacheOnly(key, _loader, indexName);
	}
	*/

	public CachedObjectWrapper<V, Z> getCachedObject(K key, CachedObjectLoader<K, V, Z, M> loader, String indexName, int start, int end) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		LockedResult lockedResult;
		synchronized (currentlyAccesingMap) {
			lockedResult = currentlyAccesingMap.get(key);
			if (lockedResult == null) {
				lockedResult = new LockedResult();
				currentlyAccesingMap.put(key, lockedResult);
			}
			lockedResult.refCount++;
		}
		try {
			return getObjectRecursively(key, caches.iterator(), loader, System.currentTimeMillis(), indexName, start, end, lockedResult, true);
		} finally {
			synchronized (currentlyAccesingMap) {
				lockedResult.refCount--;
				if (lockedResult.refCount == 0) {
					currentlyAccesingMap.remove(key);
				}
			}
		}

	}
/*
	public CachedObjectWrapper<V, Z> getCachedObjectFromCacheOnly(K key, CachedObjectLoader<K, V, Z, M> loader, String indexName) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		LockedResult lockedResult;
		synchronized (currentlyAccesingMap) {
			lockedResult = currentlyAccesingMap.get(key);
			if (lockedResult == null) {
				lockedResult = new LockedResult();
				currentlyAccesingMap.put(key, lockedResult);
			}
			lockedResult.refCount++;
		}
		try {
			return getObjectRecursively(key, caches.iterator(), loader, System.currentTimeMillis(), indexName, 0, 0, lockedResult, false);
		} finally {
			synchronized (currentlyAccesingMap) {
				lockedResult.refCount--;
				if (lockedResult.refCount == 0) {
					currentlyAccesingMap.remove(key);
				}
			}
		}

	}
	*/

	public void updateCachedObject(K key, M modificationData, String indexName, IndexWriter writer, ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		updateCachedObject(key, modificationData, _loader, indexName, writer, modType, updateStore, startDocId, endDocId);
	}

	public void updateCachedObject(K key, M modificationData, CachedObjectLoader<K, V, Z, M> loader, String indexName, IndexWriter writer, ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException, InterruptedException, MemcachedException, TimeoutException {
		LockedResult lockedResult;
		synchronized (currentlyAccesingMap) {
			lockedResult = currentlyAccesingMap.get(key);
			if (lockedResult == null) {
				lockedResult = new LockedResult();
				currentlyAccesingMap.put(key, lockedResult);
			}
			lockedResult.refCount++;
		}
		try {
			modifyObjectRecursively(key, modificationData, caches.iterator(), loader, System.currentTimeMillis(), indexName, lockedResult, writer, modType, updateStore, startDocId, endDocId);
		} finally {
			synchronized (currentlyAccesingMap) {
				lockedResult.refCount--;
				if (lockedResult.refCount == 0) {
					currentlyAccesingMap.remove(key);
				}
			}
		}

	}

	private void modifyObjectRecursively(K key, M modificationData, Iterator<VersionedCache<K, V, Z>> cacheIterator, CachedObjectLoader<K, V, Z, M> loader, long currentTime, String indexName, LockedResult lockedResult, IndexWriter writer, ModificationType modType, boolean updateStore, int startDocId, int endDocId) throws IOException {
		CachedObjectWrapper<V, Z> tmp = null;
		if (cacheIterator.hasNext()) {
			VersionedCache<K, V, Z> vc = cacheIterator.next();
			tmp = vc.get(key);
			if (tmp == null) {
				modifyObjectRecursively(key, modificationData, cacheIterator, loader, currentTime, indexName, lockedResult, writer, modType, updateStore, startDocId, endDocId);
			} else {
				lockedResult.lock.lock();
				try {
					if (lockedResult.result != null) {
						tmp = lockedResult.result;
					}
					loader.updateObject(tmp, modificationData, this, modType, startDocId, endDocId);

					if (updateStore) {
						loader.updateObjectStore(key, modificationData, writer, this, modType, startDocId, endDocId);
					}
					// TODO we aren't using the timeout, so comment out to make
					// more performant
					// resetCacheTime(key);
				} finally {
					lockedResult.lock.unlock();
				}
				// TODO we aren't using the timeout, so comment out to make more
				// performant
				// resetCacheTime(key);
			}
		} else {
			lockedResult.lock.lock();
			try {
				if (lockedResult.result != null) {
					tmp = lockedResult.result;
					loader.updateObject(tmp, modificationData, this, modType, startDocId, endDocId);
				}
				if (updateStore) {
					loader.updateObjectStore(key, modificationData, writer, this, modType, startDocId, endDocId);
				}
				
			} finally {
				lockedResult.lock.unlock();
			}

		}
	}

	private CachedObjectWrapper<V, Z> getObjectRecursively(K key, Iterator<VersionedCache<K, V, Z>> cacheIterator, CachedObjectLoader<K, V, Z, M> loader, long currentTime, String indexName, int start, int end, LockedResult lockedResult, boolean loadFromStore) throws IOException {
		CachedObjectWrapper<V, Z> tmp = null;
		if (cacheIterator.hasNext()) {
			VersionedCache<K, V, Z> vc = cacheIterator.next();
			tmp = vc.get(key);
			if (tmp == null) {
				tmp = getObjectRecursively(key, cacheIterator, loader, currentTime, indexName, start, end, lockedResult, loadFromStore);
				if (tmp != null) {
					vc.put(key, tmp);
				}
			} else {
				long cacheTime = tmp.getCacheTime();
				long diff = currentTime - cacheTime;
				if (cacheTime != -1l && diff > cacheTimeout) {
					tmp.setCacheTime(-1l);
					checkAndReloadCachedObjectAsynch(vc, cacheTime, tmp, key, cacheIterator, loader, currentTime, indexName, start, end, lockedResult, true);
                }
			}

		} else {
			if (loadFromStore) {
				lockedResult.lock.lock();
				try {
					if (lockedResult.result == null) {
						tmp = loader.loadObject(key, start, end, this);

						if(tmp != null){
							tmp.setCacheTime(System.currentTimeMillis());
							lockedResult.result = tmp;
						} else {
							logger.debug("empty object loaded for this key: " + key.toString());
						}
					} else {
						tmp = lockedResult.result;
					}
				} finally {
					lockedResult.lock.unlock();
				}
			}

		}

		return tmp;
	}
	
	private CachedObjectWrapper<V, Z> refreshObject(VersionedCache<K, V, Z> vc, long currentCacheTime, final CachedObjectWrapper<V, Z> objectWrapper, K key, Iterator<VersionedCache<K, V, Z>> cacheIterator, CachedObjectLoader<K, V, Z, M> loader, long currentTime, String indexName, int start, int end, LockedResult lockedResult, boolean loadFromStore) throws IOException {
		CachedObjectWrapper<V, Z> tmp = null;
		lockedResult.lock.lock();
		try {
			if (lockedResult.result == null) {
				tmp = loader.loadObject(key, start, end, this);
				tmp.setCacheTime(System.currentTimeMillis());
				lockedResult.result = tmp;
			} else {
				tmp = lockedResult.result;
			}
		} finally {
			lockedResult.lock.unlock();
		}
		
		vc.put(key, tmp);
		
		while (cacheIterator.hasNext()) {
			VersionedCache<K, V, Z> tmpVc = cacheIterator.next();
			tmpVc.put(key, tmp);
		}
		
		
		//objectWrapper.setCacheTime(currentCacheTime);
		
		return tmp;
	}
	

	private void checkAndReloadCachedObjectAsynch(final VersionedCache<K, V, Z> vc, final long currentCacheTime, final CachedObjectWrapper<V, Z> objectWrapper, final K key, final Iterator<VersionedCache<K, V, Z>> cacheIterator, final CachedObjectLoader<K, V, Z, M> loader, final long currentTime, final String indexName, final int start, final int end, final LockedResult lockedResult, final boolean loadFromStore) throws IOException {

		Runnable runnable = new Runnable() {

			@Override
			public void run() {
				Z loadedIdentifier;
				try {
					loadedIdentifier = loader.getVersionIdentifier(key, start, end);
					Z versionIdentifier = objectWrapper.getVersionIdentifier();

					if (loadedIdentifier == null || versionIdentifier == null || (!versionIdentifier.equals(loadedIdentifier))) {
						refreshObject(vc, currentCacheTime, objectWrapper, key, cacheIterator, loader, currentTime, indexName, start, end, lockedResult, loadFromStore);
					} else {
						resetCacheTime(key);
					}
				} catch (Throwable e) {
					logger.error(e);
				}

			}

		};
		try {
			executor.execute(runnable);
		} catch (RejectedExecutionException ree) {
			logger.warn("Async check and reload cached object failed, ignoring ", ree);
		}

	}

	public void resetCacheTime(K key) throws IOException {
		for (VersionedCache<K, V, Z> cache : caches) {
			cache.resetCacheTime(key, System.currentTimeMillis());
		}
	}

	public boolean isCacheFull() {
		VersionedCache<K, V, Z> cache = caches.get(caches.size() - 1);
		return cache.isCacheFull();
	}
}
