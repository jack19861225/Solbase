package org.solbase.lucenehbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.solbase.SolbaseByteArrayInputStream;

public class CompactedTermDocMetadataArray implements Serializable {

	private static final long serialVersionUID = 7086849173840377521L;

	// floor of buffer should be greater than equal to this val. TODO: figure out average term vector byte size is
	private static final int minTermVectorSize = 15 * 5; // in bytes. 15 bytes are average single tv, so times 5 is floor
	
	// ceil of buffer
	private static final int maxTermVectorSize = 15 * 1000; // in bytes. create buffer for 1000 docs
	
	// using percentage to buffer term vector array 
	private static final int bufferedPercentage = 5; // percentage
	
	private byte[] termVectorArray;
	private int docAmount = 0;
	public ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	
	private int termVectorSize;
	
	public void setTermDocMetadataArray(byte[] termVectorArray) {
		this.termVectorArray = termVectorArray;
	}

	public void setDocAmount(int docAmount) {
		this.docAmount = docAmount;
	}
	
	public int getTermVectorSize(){
		return this.termVectorSize;
	}
	
	public void setTermVectorSize(int termVectorSize){
		this.termVectorSize = termVectorSize;
	}

	public CompactedTermDocMetadataArray(ByteArrayOutputStream bos, int docAmount) {
		termVectorSize = bos.size();
		// make this array buffered
		int bufferedTotal = bufferTermVectorArray(bos);
		bos.write(new byte[bufferedTotal-termVectorSize], 0, bufferedTotal-termVectorSize);
		
		this.termVectorArray = bos.toByteArray();
		this.docAmount = docAmount;
	}
	
	/*
	 * @return int - remaining buffered size in byte array
	 */
	public int bufferedSize(){
		return this.termVectorArray.length - this.termVectorSize;
	}
	
	/*
	 * bufferedTermVectorArray
	 * 
	 * find proper buffered array size given input array
	 * 
	 * @param termVectorArray : initial term vector array
	 * @return int : length of buffered total size
	 */
	public static int bufferTermVectorArray(ByteArrayOutputStream bos){
		int length = bos.size();
		
		int bufferedSize = (int) Math.floor(length * (bufferedPercentage/100.0));
		if(bufferedSize > maxTermVectorSize){
			// ceil buffer size
			bufferedSize = maxTermVectorSize;
		} else if(bufferedSize < minTermVectorSize){
			// min buffer size
			bufferedSize = minTermVectorSize;
		}
		
		return length + bufferedSize;
	}

	public byte[] getTermDocMetadataArray() {
		return termVectorArray;
	}

	public int getDocAmount() {
		return docAmount;
	}
	
	public SolbaseByteArrayInputStream getTermDocMetadataInputStream() {
		return new SolbaseByteArrayInputStream(termVectorArray, termVectorSize);
	}
	
	public void deleteTermVector(int prevPosition, int currentPosition){
		System.arraycopy(termVectorArray, currentPosition, termVectorArray, prevPosition, termVectorSize-currentPosition);
		// adjust term vector size
		termVectorSize = termVectorSize - (currentPosition - prevPosition);
	}
	
	public void updateTermVector(int prevPosition, int currentPosition, byte[] newTdm){
		// prev position is the doc we are trying to update here
		int newSize = newTdm.length;
		int prevSize = currentPosition - prevPosition;
		
		if(prevSize == newTdm.length){
			// update doc is same as previous doc
			System.arraycopy(newTdm, 0, termVectorArray, prevPosition, newTdm.length);
		} else {
			// we need to find difference and move each elements of array accordingly
			if(newSize > prevSize){
				// new size is bigger than previous size
				int diff = newSize - prevSize;
				
				// move over remaining term docs
				System.arraycopy(termVectorArray, currentPosition, termVectorArray, currentPosition+diff, termVectorSize-currentPosition);
				// copy updated term doc
				System.arraycopy(newTdm, 0, termVectorArray, prevPosition, newTdm.length);

				termVectorSize += diff;
			} else {
				// new size is smaller than previous size
				int diff = prevSize - newSize;
				
				// copy updated term doc into term vector array
				System.arraycopy(newTdm, 0, termVectorArray, prevPosition, newTdm.length);
				// move remaining tdm's over
				System.arraycopy(termVectorArray, currentPosition, termVectorArray, prevPosition+newSize, termVectorSize-currentPosition);
				
				termVectorSize -= diff;
			}
		}
	}
	
	public void addTermVector(int prevPosition, int currentPosition, byte[] newTdm){
		// move over remaining bytes
		System.arraycopy(termVectorArray, prevPosition, termVectorArray, prevPosition+newTdm.length, termVectorSize-prevPosition);
		// copy new term doc into term vector array
		System.arraycopy(newTdm, 0, termVectorArray, prevPosition, newTdm.length);
		
		termVectorSize += newTdm.length;
	}
	
	private void writeObject(ObjectOutputStream out) throws IOException {
		/*
		out.write(SolbaseUtil.writeVInt(termVectorArray.length));
		for (TermDocMetadata tdm : termVectorArray) {
			out.write(SolbaseUtil.writeVInt(tdm.getDocId()));
			out.write(Bytes.toBytes(tdm.serialize()));
		}
		*/
	}
	
	private void readObject(ObjectInputStream in) throws IOException{
		/*
		int arrayLength = SolbaseUtil.mreadVInt(in);
		termVectorArray = new TermDocMetadata[arrayLength];
		
		for (int i = 0; i < arrayLength; i++) {
			termVectorArray[i] = TermDocMetadataFactory.create(in);
		}
		*/
	}
}
