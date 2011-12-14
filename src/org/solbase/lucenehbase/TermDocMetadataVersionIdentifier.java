package org.solbase.lucenehbase;

import java.io.Serializable;

public class TermDocMetadataVersionIdentifier implements Serializable{
	
	private long versionIdentifier;
	private int startDocId;
	private int endDocId;
	
	public TermDocMetadataVersionIdentifier(long versionIdentifier, int startDocId, int endDocId) {
		this.versionIdentifier = versionIdentifier;
		this.startDocId = startDocId;
		this.endDocId = endDocId;
	}

	public long getVersionIdentifier() {
		return versionIdentifier;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof TermDocMetadataVersionIdentifier){
			TermDocMetadataVersionIdentifier tdmvi = (TermDocMetadataVersionIdentifier) obj;
			return tdmvi.getStartDocId() == this.startDocId && tdmvi.getEndDocId() == this.endDocId && tdmvi.getVersionIdentifier() == this.versionIdentifier;
		} else {
			throw new UnsupportedOperationException();
		}
	}

	public int getStartDocId() {
		return startDocId;
	}

	public int getEndDocId() {
		return endDocId;
	}
	
}
