package org.solbase.cache;

import java.io.Serializable;

public class CachedObjectWrapper<V, Z> implements Serializable {

	private static final long serialVersionUID = 8345377082670614068L;
	private V value;
	private Z versionIdentifier;
	private byte[] valueBytes;
	private Long cacheTime;
	



	public CachedObjectWrapper(V value, Z versionIdentifier, Long cacheTime) {
		this.value = value;
		this.versionIdentifier = versionIdentifier;
		this.cacheTime = cacheTime;
	}


	public Long getCacheTime() {
		return cacheTime;
	}
	
	public void setCacheTime(Long cacheTime) {
		this.cacheTime = cacheTime;
	}


	public V getValue() {
		return value;
	}


	public void setValue(V value) {
		this.value = value;
	}


	public Z getVersionIdentifier() {
		return versionIdentifier;
	}


	public void setVersionIdentifier(Z versionIdentifier) {
		this.versionIdentifier = versionIdentifier;
	}

	byte[] getValueBytes() {
		return valueBytes;
	}


	void setValueBytes(byte[] valueBytes) {
		this.valueBytes = valueBytes;
	}

}
