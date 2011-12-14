package org.solbase;

import java.io.IOException;
import java.io.InputStream;

public final class SolbaseByteArrayInputStream extends InputStream{

	private byte[] bytes;
	private int pos = 0;
	private int size;
	
	public SolbaseByteArrayInputStream(byte[] bytes, int size){
		this.bytes = bytes;
		this.pos = 0;
		this.size = size;
	}
	
	@Override
	public int read() {
		return this.bytes[pos++];
	}

	@Override
	public int available() {
		return this.size - pos;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int i = 0;
		for (; i < len && pos < this.size; i++){
			b[off++] = this.bytes[pos++];
		}
		return i;
	}
	
	public int currentPostion() {
		return pos;
	}
	
}
