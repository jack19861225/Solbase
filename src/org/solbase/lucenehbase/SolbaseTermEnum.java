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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;

public class SolbaseTermEnum extends TermEnum {
    private SolbaseTermDocs termDocs;

    private String indexName;
    private int startDocId;
    private int endDocId;
    
    public SolbaseTermEnum() {
        this.termDocs = null;
    }
    
    public SolbaseTermEnum(String indexName) {
        this.termDocs = null;
        this.indexName = indexName;
    }
    
    public SolbaseTermEnum(Term term, String indexName, int startDocId, int endDocId) throws IOException {
        this.termDocs = new SolbaseTermDocs(term, indexName, startDocId, endDocId);
        this.indexName = indexName;
        this.startDocId = startDocId;
        this.endDocId = endDocId;
    }
    /*
    public SolbaseTermEnum(Term term, SolbaseByteArrayInputStream termDocs) {
        this.termDocs = new SolbaseTermDocs(term, termDocs, indexName, startDocId, endDocId);
    }
    */

    public boolean skipTo(Term term) throws IOException {
        if (termDocs != null && termDocs.getTerm().equals(term)) {
            return true;
        }
        this.termDocs = new SolbaseTermDocs(term, indexName, startDocId, endDocId);
        return this.termDocs.docAmount() > 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public int docFreq() {
        return termDocs == null ? 0 : termDocs.docAmount();
    }

    @Override
    public boolean next() throws IOException {
        return false;//throw new UnsupportedOperationException();
    }

    @Override
    public Term term() {
        return termDocs == null ?  null : this.termDocs.getTerm();
    }
    
    public SolbaseTermDocs getTermDocs() {
        return termDocs;
    }
}
