/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.solbase.lucenehbase;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Scorer;
import org.solbase.SolbaseUtil;

public class TermDocMetadata implements Serializable {

	private static final long serialVersionUID = -7706773478353560562L;
	
	public static final ByteBuffer positionVectorKeyBytes = ByteBuffer.wrap(Bytes.toBytes("P"));
	public static final ByteBuffer offsetVectorKeyBytes = ByteBuffer.wrap(Bytes.toBytes("O"));
	public static final ByteBuffer termFrequencyKeyBytes = ByteBuffer.wrap(Bytes.toBytes("F"));
	public static final ByteBuffer normsKeyBytes = ByteBuffer.wrap(Bytes.toBytes("N"));
	public static final ByteBuffer sortFieldKeyBytes = ByteBuffer.wrap(Bytes.toBytes("S"));
	
	public static final int initialSortSlots = 5;
	
	public int docId;
	public int freq = -1;	
	public Byte norm = null;
	public int[] positions = null;
	public int[] offsets = null;
	public int[] sortValues;
	
	private Term term;
	
	public TermDocMetadataVersionIdentifier versionIdentifier;
	
	private byte[] fieldTermKeyBytes;
	
	public TermDocMetadata(int docId, int freq) {
		this.docId = docId;
		this.freq = freq;
	}
	
	public TermDocMetadata(int docId, byte[] aBytes) {
		ByteBuffer bytes_ = ByteBuffer.wrap(aBytes);
		ByteBuffer bytes = bytes_.duplicate(); // don't mutate the original

		create(docId, bytes);
	}
	
	public TermDocMetadata(int docId, ByteBuffer bytes) {
		create(docId, bytes);
	}

	public TermDocMetadata(InputStream bb) throws IOException {
		int docId = SolbaseUtil.mreadVInt(bb);
		create(docId, bb);
	}
	
	public TermDocMetadata(int docId, InputStream bytes) throws IOException{
		create(docId, bytes);
	}
	
	public TermDocMetadata(int docId, Map<ByteBuffer, List<Number>> data, byte[] fieldTermKeyBytes, Term term){
		this.docId = docId;
		this.term = term;
		this.fieldTermKeyBytes = fieldTermKeyBytes;
		
		for (Map.Entry<ByteBuffer, List<Number>> e : data.entrySet()) {
			assert e.getKey() != null;

			if (e.getKey().equals(normsKeyBytes)) {
				norm = e.getValue().get(0).byteValue();
			}

			else if (e.getKey().equals(positionVectorKeyBytes)) {
				List<Number> p = e.getValue();

				positions = new int[p.size()];
				for (int i = 0; i < positions.length; i++)
					positions[i] = p.get(i).intValue();
			}

			else if (e.getKey().equals(offsetVectorKeyBytes)) {
				List<Number> o = e.getValue();

				offsets = new int[o.size()];
				for (int i = 0; i < offsets.length; i++)
					offsets[i] = o.get(i).intValue();
			}

			else if (e.getKey().equals(termFrequencyKeyBytes)) {
				List<Number> value = e.getValue();

				freq = value.size() == 0 ? 0 : value.get(0).intValue();
			}

			else if (e.getKey().equals(sortFieldKeyBytes)){
				List<Number> sortValues = e.getValue();
				this.sortValues = new int[Scorer.numSort];
				
				for(int i = 0; i < Scorer.numSort; i++){
					int val = (Integer) sortValues.get(i);
					this.sortValues[i] = val;
				}
			}
			
			else {
				throw new IllegalArgumentException(Bytes.toString(e.getKey().array()));
			}
		}

		if (freq < 0)
			throw new IllegalArgumentException("term freq is < 0");

		if (positions != null && freq != positions.length)
			throw new IllegalArgumentException("freq != position count: " + freq + " vs " + positions.length);
		
	}
	
	public void create(int docId, InputStream bytes) throws IOException {

		byte flags;
		// add in more flag bytes if we have more than default 5 sortable fields
		if(Scorer.numSort > initialSortSlots){
			float diff = Scorer.numSort - initialSortSlots;
			int numBytes = (int) Math.ceil(diff / 8);
			flags = (byte) bytes.read();
			
			while(numBytes > 0){
				bytes.read();
				numBytes--;
			}
		} else {
			flags = (byte) bytes.read();
		}

		boolean hasNorm = (flags & 1) == 1;
		boolean hasPositions = (flags & 2) == 2;
		boolean hasOffsets = (flags & 4) == 4;

		freq = SolbaseUtil.mreadVInt(bytes);

		norm = hasNorm ? ((byte)bytes.read()) : null;

		int[] positions_ = null;
		if (hasPositions) {
			positions_ = new int[freq];

			for (int i = 0; i < freq; i++)
				positions_[i] = SolbaseUtil.mreadVInt(bytes);

		}

		positions = positions_;

		int[] offsets_ = null;
		if (hasOffsets) {
			int len = SolbaseUtil.mreadVInt(bytes);
			offsets_ = new int[len];

			for (int i = 0; i < len; i++)
				offsets_[i] = SolbaseUtil.mreadVInt(bytes);
		}

		offsets = offsets_;
	}

	public void create(int docId, ByteBuffer bytes) {
		this.docId = docId;
		
		byte flags;
		// add in more flag bytes if we have more than default 5 sortable fields
		if(Scorer.numSort > initialSortSlots){
			float diff = Scorer.numSort - initialSortSlots;
			int numBytes = (int) Math.ceil(diff / 8);
			flags = bytes.get();
			
			while(numBytes > 0){
				bytes.get();
				numBytes--;
			}
		} else {
			flags = bytes.get();
		}

		boolean hasNorm = (flags & 1) == 1;
		boolean hasPositions = (flags & 2) == 2;
		boolean hasOffsets = (flags & 4) == 4;

		freq = SolbaseUtil.mreadVInt(bytes);

		norm = hasNorm ? bytes.get() : null;

		int[] positions_ = null;
		if (hasPositions) {
			positions_ = new int[freq];

			for (int i = 0; i < freq; i++)
				positions_[i] = SolbaseUtil.mreadVInt(bytes);

		}

		positions = positions_;

		int[] offsets_ = null;
		if (hasOffsets) {
			int len = SolbaseUtil.mreadVInt(bytes);
			offsets_ = new int[len];

			for (int i = 0; i < len; i++)
				offsets_[i] = SolbaseUtil.mreadVInt(bytes);
		}

		offsets = offsets_;
	}
	
	public int getDocId() {
		return this.docId;
	}
	
	protected boolean hasPositions() {
		return positions != null && positions.length != 0;
	}

	public boolean hasOffsets() {
		return offsets != null && offsets.length != 0;
	}
	
	protected int[] getPositions() {
		return null;
	}

	protected int[] getOffsets() {
		return null;
	}

	protected boolean hasNorm() {
		return true;
	}
	
	public byte getNorm() {
		return norm;
	}
	
	protected boolean hasSortField(int index){
		return sortValues[index] != -1;
	}
	
	public int getSortValue(int index){
		return sortValues[index];
	}
	
	public byte[] getFieldTermKey() {
		return fieldTermKeyBytes;
	}

	public ByteBuffer serialize() {		
		// flags, freq, norm, pos, numoff, off
		int size = 1 + 4 + (hasNorm() ? 1 : 0) + (hasPositions() ? getPositions().length * 4 : 0) + (hasOffsets() ? getOffsets().length * 4 + 4 : 0);
		
		// store the initial content flags in the inital byte
		byte flags = 0;
		if (hasNorm())
			flags |= 1;

		if (hasPositions())
			flags |= 2;

		if (hasOffsets())
			flags |= 4;

		int sortFieldSize = 0;

		ByteBuffer flagBytes;
		// add in more flag bytes if we have more than default 5 sortable fields
		if(Scorer.numSort > initialSortSlots){
			float diff = Scorer.numSort - initialSortSlots;
			int numBytes = (int) Math.ceil(diff / 8);
			size += numBytes;
			flagBytes = ByteBuffer.allocate(1+numBytes);
		} else {
			flagBytes = ByteBuffer.allocate(1);
		}
		
		for(int i = 0; i < Scorer.numSort; i++){
			if((i + 3) % 8 == 0){
				flagBytes.put(flags);
				flags = 0;
			}
			
			if(i < 5){
				// use remaining 5 bits
				if (hasSortField(i)) {
					flags |= (1 << (i + 3));
					sortFieldSize += 4;
				}
			} else {
				// allocating new byte to support more sortable fields
				int j = i + 3;
				j %= 8;
				if(hasSortField(i)){
					flags |= (1 << j);
					sortFieldSize += 4;
				}
			}
		}
		
		// insert last flag byte into buffer
		flagBytes.put(flags);
		
		// adding in each individual sortable fields bytes (4 bytes per field - integer)
		size += sortFieldSize;
		
		ByteBuffer r = ByteBuffer.allocate(size);
		r.put(flagBytes.array());
		r.put(SolbaseUtil.writeVInt(freq));

		if (hasNorm())
			r.put(getNorm());

		if (hasPositions()) {
			for (int i = 0; i < getPositions().length; i++) {
				r.put(SolbaseUtil.writeVInt(getPositions()[i]));
			}
		}

		if (hasOffsets()) {
			r.put(SolbaseUtil.writeVInt(getOffsets().length));

			for (int i = 0; i < getOffsets().length; i++) {
				r.put(SolbaseUtil.writeVInt(getOffsets()[i]));
			}
		}
		
		for(int i = 0; i < Scorer.numSort; i++){
			if (hasSortField(i)){
				byte[] sortValue = SolbaseUtil.writeVInt(getSortValue(i));
				
				r.put(sortValue);
				
				SolbaseUtil.mreadVInt(ByteBuffer.wrap(sortValue));
			}
		}

		r.flip();

		return r;
	}
	
	public Term getTerm(){
		return this.term;
	}

}
