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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermVectorOffsetInfo;
import org.solbase.SolbaseUtil;

public class TermFreqVector implements org.apache.lucene.index.TermFreqVector, org.apache.lucene.index.TermPositionVector {

    private String field;
    private byte[] docId;
    private String[] terms;
    private int[] freqVec;
    private int[][] termPositions;
    private TermVectorOffsetInfo[][] termOffsets;

    public TermFreqVector(String field, int docIdInt) throws IOException, ClassNotFoundException {
        this.field = field;
        this.docId = SolbaseUtil.writeVInt(docIdInt);

        HTableInterface docTable = SolbaseUtil.getDocTable();
        HTableInterface termVectorTable = SolbaseUtil.getTermVectorTable();
 
        try {
            
            Get documentGet = new Get(docId);
            documentGet.addColumn(Bytes.toBytes("allTerms"), Bytes.toBytes("allTerms"));

            Result documentResult = docTable.get(documentGet);

            if (documentResult.isEmpty()) {

                return; // this docId is missing
            }

            @SuppressWarnings("unchecked")
			List<Term> allTerms  = (List<Term>) SolbaseUtil.fromBytes(ByteBuffer.wrap(documentResult.getValue(Bytes.toBytes("allTerms"), Bytes.toBytes("allTerms"))));
          
            List<Result> termResults = new ArrayList<Result>();
            
            for (Term t : allTerms) {
                byte[] termVecKey = Bytes.add(SolbaseUtil.generateTermKey(t), SolbaseUtil.delimiter, docId); 
                Get germVectorGet = new Get(termVecKey);

                Result termVecGetReult = docTable.get(germVectorGet);
                termResults.add(termVecGetReult);
            }
            

            terms = new String[termResults.size()];
            freqVec = new int[termResults.size()];
            termPositions = new int[termResults.size()][];
            termOffsets = new TermVectorOffsetInfo[termResults.size()][];

            int i = 0;

            for (Result row : termResults) {
                
                byte[] fieldName = row.getValue(Bytes.toBytes("field"), Bytes.toBytes("field"));
                byte[] termTextName = row.getValue(Bytes.toBytes("term"), Bytes.toBytes("term"));

                Term t = new Term(Bytes.toString(fieldName), Bytes.toString(termTextName));

                terms[i] = t.text();

                byte[] documentTermInfo = row.getValue(Bytes.toBytes("document"), docId);
                // Find the offsets and positions
                TermDocMetadata termInfo = new TermDocMetadata(0, documentTermInfo);

                termPositions[i] = termInfo.getPositions();
                

                freqVec[i] = termPositions[i].length;

                if (termInfo == null || !termInfo.hasOffsets()) {
                    termOffsets[i] = TermVectorOffsetInfo.EMPTY_OFFSET_INFO;
                } else {

                    int[] offsets = termInfo.getOffsets();

                    termOffsets[i] = new TermVectorOffsetInfo[freqVec[i]];
                    for (int j = 0, k = 0; j < offsets.length; j += 2, k++) {
                        termOffsets[i][k] = new TermVectorOffsetInfo(offsets[j], offsets[j + 1]);
                    }
                }

                i++;
            }
        } finally {
            SolbaseUtil.releaseTable(docTable);
            SolbaseUtil.releaseTable(termVectorTable);
        }

    }

    public String getField() {
        return field;
    }

    public int[] getTermFrequencies() {
        return freqVec;
    }

    public String[] getTerms() {
        return terms;
    }

    public int indexOf(String term) {
        return Arrays.binarySearch(terms, term);
    }

    public int[] indexesOf(String[] terms, int start, int len) {
        int[] res = new int[terms.length];

        for (int i = 0; i < terms.length; i++) {
            res[i] = indexOf(terms[i]);
        }

        return res;
    }

    public int size() {
        return terms.length;
    }

    public TermVectorOffsetInfo[] getOffsets(int index) {
        return termOffsets[index];
    }

    public int[] getTermPositions(int index) {
        return termPositions[index];
    }

}
