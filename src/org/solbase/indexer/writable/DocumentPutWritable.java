package org.solbase.indexer.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.lucene.index.Term;
import org.solbase.SolbaseUtil;

public class DocumentPutWritable implements Writable{

	int docId;

	String globalId;
	
	List<String> fieldKeys;
	byte[] fieldKeysBytes;
	int fieldKeysLength;
	
	List<byte[]> fieldValues;
	byte[] fieldValuesBytes;
	int fieldValuesLength;

	List<Term> allTerms;
	byte[] allTermsBytes;
	int allTermsLength;
	
	public int getDocId(){
		return this.docId;
	}
	
	public List<Term> getAllTerms(){
		return this.allTerms;
	}
	
	public List<String> getFieldKeys(){
		return this.fieldKeys;
	}
	
	public List<byte[]> getFieldValues(){
		return this.fieldValues;
	}
	
	public String getGlobalId(){
		return this.globalId;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(docId);
		if (docId != -1) {
			out.writeUTF(globalId);
			out.writeInt(fieldKeysLength);
			out.write(fieldKeysBytes);
			out.writeInt(fieldValuesLength);
			out.write(fieldValuesBytes);

			out.writeInt(allTermsLength);
			out.write(allTermsBytes);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		docId = in.readInt();
		if (docId != -1) {
			globalId = in.readUTF();
			fieldKeysLength = in.readInt();
			fieldKeysBytes = new byte[fieldKeysLength];
			in.readFully(fieldKeysBytes);

			fieldValuesLength = in.readInt();
			fieldValuesBytes = new byte[fieldValuesLength];
			in.readFully(fieldValuesBytes);

			allTermsLength = in.readInt();
			allTermsBytes = new byte[allTermsLength];
			in.readFully(allTermsBytes);
			try {
				allTerms = (List<Term>) SolbaseUtil.fromBytes(ByteBuffer.wrap(allTermsBytes));
				fieldKeys = (List<String>) SolbaseUtil.fromBytes(ByteBuffer.wrap(fieldKeysBytes));
				fieldValues = (List<byte[]>) SolbaseUtil.fromBytes(ByteBuffer.wrap(fieldValuesBytes));

			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public DocumentPutWritable(){
		docId = -1;
	}
	
	public DocumentPutWritable(Set<Map.Entry<String, byte[]>> fieldsMap, List<Term> allTerms, int docId, String globalId){
		this.docId = docId;
		this.globalId = globalId;
		
		this.allTerms = allTerms;
		
		fieldKeys = new ArrayList<String>();
		fieldValues = new ArrayList<byte[]>();
		
		// Store each field as a column under this docId
		for (Map.Entry<String, byte[]> field : fieldsMap) {
			fieldKeys.add(field.getKey());
			fieldValues.add(field.getValue());
		}
		
		try {
			fieldKeysBytes = SolbaseUtil.toBytes(fieldKeys).array();
			fieldKeysLength = fieldKeysBytes.length;
			
			fieldValuesBytes = SolbaseUtil.toBytes(fieldValues).array();
			fieldValuesLength = fieldValuesBytes.length;

			ByteBuffer allTermsBuff = SolbaseUtil.toBytes(allTerms);
			this.allTermsBytes = allTermsBuff.array();
			this.allTermsLength = allTermsBytes.length;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
