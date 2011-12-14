package org.solbase.indexer.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TermDocMetadataWritable implements Writable {

	int docId;
	int termDocMetaSize;
	byte[] termDocMetadata;
	
	int fieldTermKeySize;
	byte[] fieldTermKey;
	
	public TermDocMetadataWritable(){
		docId = -1;
	}
	
	public TermDocMetadataWritable(int docId, byte[] termDocMetadata, byte[] fieldTermKey){
		this.docId = docId;
		this.termDocMetadata = termDocMetadata;
		this.termDocMetaSize = termDocMetadata.length;
		this.fieldTermKey = fieldTermKey;
		this.fieldTermKeySize = fieldTermKey.length;
	}
	
	public byte[] getFieldTermKey(){
		return this.fieldTermKey;
	}
	
	public int getDocId(){
		return this.docId;
	}
	
	public byte[] getTermDocMetadata(){
		// this data is already serialized
		return this.termDocMetadata;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(docId);
		if(docId != -1){
			out.writeInt(termDocMetaSize);
			out.write(termDocMetadata);
			out.writeInt(fieldTermKeySize);
			out.write(fieldTermKey);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		docId = in.readInt();
		if(docId != -1){
			termDocMetaSize = in.readInt();
			termDocMetadata = new byte[termDocMetaSize];
			in.readFully(termDocMetadata);
			fieldTermKeySize = in.readInt();
			fieldTermKey = new byte[fieldTermKeySize];
			in.readFully(fieldTermKey);
		}
	}
	
    public static TermDocMetadataWritable read(DataInput in) throws IOException {
    	TermDocMetadataWritable w = new TermDocMetadataWritable();
        w.readFields(in);
        return w;
    }
}
