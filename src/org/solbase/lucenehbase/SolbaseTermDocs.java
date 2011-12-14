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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.TermPositions;
import org.apache.lucene.search.Scorer;
import org.solbase.SolbaseByteArrayInputStream;
import org.solbase.SolbaseUtil;

public class SolbaseTermDocs implements TermDocs, TermPositions {

	private Term term;
	public SolbaseByteArrayInputStream termDocs;

	private int currentDocTermIterationPosition;
	private String indexName;
	private int startDocId;
	private int endDocId;
	private int docAmount = 0;

	private int docId;

	private Byte norm = null;
	private int[] positions = null;
	private int[] offsets = null;
	private int freq = -1;
	private int[] sortValues = new int[Scorer.numSort];

	CompactedTermDocMetadataArray compactedTermDocMetadataArray = null;

	public SolbaseTermDocs() {
		this.termDocs = null;
		this.term = null;
		this.currentDocTermIterationPosition = -1;
	}

	public SolbaseTermDocs(String indexName) {
		this.termDocs = null;
		this.term = null;
		this.currentDocTermIterationPosition = -1;
		this.indexName = indexName;
	}

	public SolbaseTermDocs(Term term, String indexName, int start, int end) throws IOException {
		this.term = term;
		this.indexName = indexName;
		this.startDocId = start;
		this.endDocId = end;
		loadTerm(term);
	}

	public SolbaseTermDocs(CompactedTermDocMetadataArray compactedTermDocMetadataArray) {
		this.compactedTermDocMetadataArray = compactedTermDocMetadataArray;
		this.termDocs = compactedTermDocMetadataArray.getTermDocMetadataInputStream();
	}

	public Term getTerm() {
		return term;
	}

	public int docAmount() {
		return this.docAmount;
	}

	private void loadTerm(Term term) throws IOException {

		if (term == null) {
			return;
		}

		try {
			CompactedTermDocMetadataArray tvArray = ReaderCache.getTermDocsMetadata(term, this.indexName, this.startDocId, this.endDocId).getValue();
			this.compactedTermDocMetadataArray = tvArray;
			tvArray.readWriteLock.readLock().lock();
			try {
				this.termDocs = tvArray.getTermDocMetadataInputStream();
				this.docAmount = tvArray.getDocAmount();
			} finally {
				tvArray.readWriteLock.readLock().unlock();
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		currentDocTermIterationPosition = -1;
	}

	public void close() throws IOException {
	}

	public int doc() {
		return docId;
	}

	public int freq() {
		return freq;
	}

	public Byte norm() {
		return norm;
	}

	public int[] offsets() {
		return offsets;
	}

	private void readTermDocMetadata() {
		// we can read vint here and do accordingly for flags.
		byte flags = (byte) termDocs.read();

		boolean hasNorm = (flags & 1) == 1;
		boolean hasPositions = (flags & 2) == 2;
		boolean hasOffsets = (flags & 4) == 4;

		boolean[] hasSortFields = new boolean[sortValues.length];
		for (int i = 0; i < sortValues.length; i++) {
			if((i + 3) % 8 == 0){	
				flags = (byte) termDocs.read();
			}
			
			if(i < 5){
				hasSortFields[i] = (flags & (1 << (i + 3))) == (1 << (i + 3));
			} else {
				// if it has more than 5 sortable fields
				int j = i + 3;
				j %= 8;

				hasSortFields[i] = (flags & (1 << j)) == (1 << j);
			}
		}

		try {
			freq = SolbaseUtil.mreadVInt(termDocs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		norm = hasNorm ? ((byte) termDocs.read()) : null;

		int[] positions_ = null;
		if (hasPositions) {
			positions_ = new int[freq];

			for (int i = 0; i < freq; i++) {
				try {
					positions_[i] = SolbaseUtil.mreadVInt(termDocs);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		positions = positions_;

		int[] offsets_ = null;
		if (hasOffsets) {
			int len;
			try {
				len = SolbaseUtil.mreadVInt(termDocs);

				offsets_ = new int[len];

				for (int i = 0; i < len; i++)
					offsets_[i] = SolbaseUtil.mreadVInt(termDocs);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		offsets = offsets_;

		for (int i = 0; i < sortValues.length; i++) {
			if (hasSortFields[i]) {
				try {
					sortValues[i] = SolbaseUtil.mreadVInt(termDocs);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// setting it to negative one instead zero
				sortValues[i] = -1;
			}
		}

	}

	public boolean next() throws IOException {

		if (termDocs == null || termDocs.available() <= 0)
			return false;

		byte[] docId = new byte[4];
		termDocs.read(docId, 0, docId.length);
		readTermDocMetadata();
		this.docId = Bytes.toInt(docId);

		return true;
	}

	// with our custom sort field implementation, this method shouldn't be
	// called
	// but needed for abstract method implementation this class is extending
	public int read(int[] docs, int[] freqs) throws IOException {
		int i = 0;

		for (; (termDocs != null && termDocs.available() > 0 && i < docs.length); i++) {
			next();
			docs[i] = doc();
			freqs[i] = freq();

		}

		return i;
	}

	public int read(int[] docs, int[] freqs, byte[] norms, int[][] sortFields) throws IOException {
		int i = 0;

		for (; (termDocs != null && termDocs.available() > 0 && i < docs.length); i++) {
			next();
			docs[i] = doc();
			freqs[i] = freq();
			norms[i] = norm();

			// we could optimize this by using rolling it instead of iterating
			// it since we know there are only going to be
			// at most five sortable fields for now
			for (int j = 0; j < sortValues.length; j++) {
				sortFields[i][j] = sortValues[j];
			}
		}

		return i;
	}

	public void seek(Term term) throws IOException {
		loadTerm(term);
	}

	public void seek(TermEnum termEnum) throws IOException {
		loadTerm(termEnum.term());
	}

	// this should be used to find a already loaded doc
	public boolean skipTo(int target) throws IOException {
		do {
			if (!next())
				return false;
		} while (target > this.docId);
		return true;
	}

	public byte[] getPayload(byte[] data, int offset) throws IOException {
		throw new UnsupportedOperationException();
	}

	public int getPayloadLength() {
		throw new UnsupportedOperationException();
	}

	public boolean isPayloadAvailable() {
		throw new UnsupportedOperationException();
	}

	public int nextPosition() throws IOException {
		currentDocTermIterationPosition++;

		if (termDocs.available() <= 0 || positions == null || currentDocTermIterationPosition >= positions.length) {
			currentDocTermIterationPosition--;
			return -1;
		}

		return positions[currentDocTermIterationPosition];

	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public int[] getSorts() {
		return this.sortValues;
	}

	@Override
	public int getSort(int fieldNumber) {
		return this.sortValues[fieldNumber];
	}

}
