package org.solbase.cache;

import java.util.ArrayList;


public class MemcacheCachedObjectWrapper<V, Z> extends CachedObjectWrapper<V, Z> {

	private static final long serialVersionUID = 152298988229290352L;

	private ArrayList<String> uuids;

	public MemcacheCachedObjectWrapper(V value, Z versionIdentifier, Long cacheTime) {
		super(value, versionIdentifier, cacheTime);
	}
	
	ArrayList<String> getUuids() {
		return uuids;
	}


	void setUuids(ArrayList<String> uuids) {
		this.uuids = uuids;
	}
}
