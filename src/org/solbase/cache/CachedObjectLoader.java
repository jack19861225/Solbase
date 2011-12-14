package org.solbase.cache;

import java.io.IOException;
import java.io.Serializable;

import org.solbase.lucenehbase.IndexWriter;


public interface CachedObjectLoader<K extends Serializable & Comparable<?>, V extends Serializable, Z extends Serializable, M extends Serializable> {
	public CachedObjectWrapper<V, Z> loadObject(K key, int start, int end, LayeredCache<K,V,Z, M> cache) throws IOException;
	public void updateObject(CachedObjectWrapper<V, Z>  object, M modificationdData, LayeredCache<K,V,Z,M> cache, LayeredCache.ModificationType modType, int startDocId, int endDocId) throws IOException;
	public void updateObjectStore(K key, M modificationdData, IndexWriter writer, LayeredCache<K,V,Z,M> cache, LayeredCache.ModificationType modType, int startDocId, int endDocId) throws IOException;
	
	public Z getVersionIdentifier(K key, int startDocId, int endDocId) throws IOException;
}
