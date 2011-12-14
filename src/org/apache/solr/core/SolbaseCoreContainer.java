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
package org.apache.solr.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.solr.schema.IndexSchema;
import org.solbase.SolbaseShardUtil;
import org.solbase.SolbaseUtil;
import org.solbase.lucenehbase.ReaderCache;
import org.xml.sax.SAXException;

public class SolbaseCoreContainer extends CoreContainer {
    private static final Logger logger = Logger.getLogger(SolbaseCoreContainer.class);
    public final static Pattern shardPattern = Pattern.compile("^([^~]+)~?(\\d*)$");

    private final String solrConfigFile;
    private final SolrCore singleCore;

    private HashMap<String, SolrCore> cache = new HashMap<String, SolrCore>();

    public SolbaseCoreContainer(String solrConfigFile) throws ParserConfigurationException, IOException, SAXException {
        this.solrConfigFile = solrConfigFile;

        SolrConfig solrConfig = new SolrConfig(solrConfigFile);
        CoreDescriptor dcore = new CoreDescriptor(null, "", ".");

        singleCore = new SolrCore("", "/tmp/search/solr-hbase/data", solrConfig, null, dcore);
        
        ReaderCache.schema = singleCore.getSchema();
    }

    @Override
    public SolrCore getCore(String name) {

        logger.debug("Loading Solbase core: " + name);

        SolrCore core = null;

        if (name.equals("")) {
            core = singleCore;
        } else {

            Matcher m = shardPattern.matcher(name);
            if (!m.find())
                throw new RuntimeException("Invalid indexname: " + name);

            try {
                core = readSchema(name, m.group(1));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ParserConfigurationException e) {
                throw new RuntimeException(e);
            } catch (SAXException e) {
                throw new RuntimeException(e);
            }
        }

        if (core != null) {
            synchronized (core) {
                core.open();
            }
        }

        return core;
    }

    public static String readSchemaXML(String indexName) throws IOException {

        return Bytes.toString(readSchemaXMLBytes(indexName));
    }

    public static byte[] readSchemaXMLBytes(String indexName) throws IOException {
        HTableInterface table = SolbaseUtil.getSchemaInfoTable();
        try {
        	int idx = indexName.indexOf("~");
        	
        	if(idx >= 0){
        		indexName = indexName.substring(0, idx);
        	}
        	
            Get schemaGet = new Get(Bytes.toBytes(indexName));
            Result schemaQueryResult = table.get(schemaGet);
            byte[] schemaValue = schemaQueryResult.getValue(Bytes.toBytes("info"), Bytes.toBytes("schema"));
            return schemaValue;
        } finally {
            SolbaseUtil.releaseTable(table);
        }

    }

    public synchronized SolrCore readSchema(String indexName, String schemaName) throws IOException, ParserConfigurationException, SAXException {

        SolrCore core = cache.get(indexName);

        if (core == null) {
            logger.debug("loading indexInfo for: " + indexName);

            byte[] schemaBytes = readSchemaXMLBytes(indexName);
            
            if(schemaBytes == null){
            	throw new IOException("schema doesn't exist for indexName: " + indexName);
            }
            
            ByteBuffer buf = ByteBuffer.wrap(schemaBytes);
            
            InputStream stream = new ByteArrayInputStream(buf.array(), buf.position(), buf.remaining());
            SolrConfig solrConfig = new SolrConfig(solrConfigFile);
            IndexSchema schema = new IndexSchema(solrConfig, schemaName, stream);

            core = new SolrCore(indexName, "/tmp/search/solr-hbase/data", solrConfig, schema, null);
            
            logger.debug("Loaded core from hbase: " + indexName);

            cache.put(indexName, core);
        }

        return core;
    }

    public static void writeSchema(String indexName, String schemaXml) throws IOException {

        HTableInterface table = SolbaseUtil.getSchemaInfoTable();
        try {
            Put schemaPut = new Put(Bytes.toBytes(indexName));
    
            schemaPut.add(Bytes.toBytes("info"), Bytes.toBytes("schema"), Bytes.toBytes(schemaXml));
    
            table.put(schemaPut);
        } finally {
            SolbaseUtil.releaseTable(table);
        }
    }

    @Override
    public Collection<String> getCoreNames(SolrCore core) {
        return Arrays.asList(core.getName());
    }

}
