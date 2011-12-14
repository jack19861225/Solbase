package org.solbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ResourceBundle;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.SolbaseHTablePool;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.lucene.index.Term;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

/**
 * @author koh
 *
 * needs to create these tables first
 * 
 * create 'SI', 'info', {NAME=>'info',REPLICATION_SCOPE=>1,VERSION=>1}
 * create 'Docs', 'field', 'allTerms', 'timestamp', {COMPRESSION=>'SNAPPY',NAME=>'field',VERSION=>1,REPLICATION_SCOPE=>1},{COMPRESSION=>'SNAPPY',NAME=>'allTerms',VERSION=>1,REPLICATION_SCOPE=>1},{COMPRESSION=>'SNAPPY',NAME=>'timestamp',VERSION=>1, REPLICATION_SCOPE=>1}
 * d => document, t=>term, f=>field
 * create 'TV', 'd', {COMPRESSION=>'SNAPPY',NAME=>'d',VERSION=>1, REPLICATION_SCOPE=>1} 
 * create 'DocKeyIdMap', 'docId',{COMPRESSION=>'SNAPPY',NAME=>'docId',VERSION=>1,REPLICATION_SCOPE=>1}
 * create 'Sequence', 'id',{COMPRESSION=>'SNAPPY',NAME=>'id',VERSION=>1,REPLICATION_SCOPE=>1}
 * create 'TVVersionId', 'timestamp', {COMPRESSION=>'SNAPPY',NAME=>'timestamp',VERSION=>1,REPLICATION_SCOPE=>1}
 * create 'uniq_checksum_user_media', 'userMediaKey', {COMPRESSION=>'SNAPPY',NAME=>'userMediaKey',VERSION=>1,REPLICATION_SCOPE=>1}
 * 
 * loading solr schema file to solbase
 * curl http://localhost:8080/solbase/schema/pbimages --data-binary @image_schema.xml -H 'Content-type:text/xml; charset=utf-8' 
 */
public final class SolbaseUtil {

	public static final byte[] delimiter = {Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE };
	//public static final byte[] delimiter = {-17, -65, -65};
	
	// used to save bytes on current time minutes (2005/1/1)
	public static final long SolbaseEpochTime = 18408960l;
	
	public static final byte[] floorBytes = {0, 0, 0, 0 };

	public static final byte[] termVectorTable;
	public static final byte[] docTable;
	public static final byte[] schemaInfoTable;
	public static final byte[] docKeyIdMapTable;
	public static final byte[] sequenceTable;
	public static final byte[] termVectorVersionIDTable;
	public static final byte[] uniqChecksumUserMediaTable;
	public static final byte[] userMediaTable;
	
	static {
		String dbPostfix = System.getProperty("solbase.db.postfix");
		if(dbPostfix == null && ResourceBundle.getBundle("solbase") != null){
			dbPostfix = ResourceBundle.getBundle("solbase").getString("db.postfix");
		}

		dbPostfix = (dbPostfix == null || dbPostfix.isEmpty()) ? "" : "_" + dbPostfix;
		termVectorTable = Bytes.toBytes("TV" + dbPostfix);
		docTable = Bytes.toBytes("Docs" + dbPostfix);
		schemaInfoTable = Bytes.toBytes("SI");
		docKeyIdMapTable = Bytes.toBytes("DocKeyIdMap" + dbPostfix);
		sequenceTable = Bytes.toBytes("Sequence" + dbPostfix);
		termVectorVersionIDTable = Bytes.toBytes("TVVersionId" + dbPostfix);
		uniqChecksumUserMediaTable = Bytes.toBytes("uniq_checksum_user_media" + dbPostfix);
		userMediaTable = Bytes.toBytes("user_media");
	}

	public static final int UNIQ_ID_CHUNK = 10000;
	
	public static final byte[] timestampColumnFamilyName = Bytes.toBytes("timestamp");
	 
    public static final byte[] termVectorDocColumnFamilyName = Bytes.toBytes("d");
	
	public static final byte[] docIdColumnFamilyName = Bytes.toBytes("docId");
	
	public static final byte[] idColumnFamilyName = Bytes.toBytes("id");
	
	public static final byte[] allTermsColumnFamilyName = Bytes.toBytes("allTerms");
	
	public static final byte[] fieldColumnFamilyName = Bytes.toBytes("field");
	
	public static final byte[] userMediaKeyColumnFamilyName = Bytes.toBytes("userMediaKey");
	
	public static final byte[] tombstonedColumnFamilyQualifierBytes = Bytes.toBytes("tombstoned");
	
	public static final byte[] emptyColumnFamilyQualifierBytes = Bytes.toBytes("");
	
	
	public static int   cacheInvalidationInterval = 1000;//ms
	
	private static int SOLBASE_HTABLE_POOL = 100; // per table. 3 (docs, tv, tvversionid) * 10 (num of region servers) * 100 = 3000 threads ~ 3.0G at most

	private static SolbaseHTablePool hTablePool;

	private static Configuration conf;
	
	static {
		conf = HBaseConfiguration.create();
		hTablePool = new SolbaseHTablePool(conf, SOLBASE_HTABLE_POOL);
	}

	public static HTableInterface getTable(byte[] tableName) {
		return hTablePool.getTable(tableName);
	}
	
	public static HTable getLocalTable(byte[] tableName){
		HTable table = (HTable)hTablePool.getTable(tableName);
		/*
		try {
			// setting buffer size to 12MB
			table.setWriteBufferSize(1024*1024*12);
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
		table.setAutoFlush(false);
		return table;
	}
	
	public static HTableInterface getTermVectorTable() {
		return getTable(termVectorTable);
	}
	
	public static HTable getLocalTermVectorTable(){
		return getLocalTable(termVectorTable);
	}
	
	public static String getTermVectorTableName(){
		return Bytes.toString(termVectorTable);
	}
	
	// TODO: uniqChecksumUserMediaTable is PB specific table
	public static HTableInterface getUniqChecksumUserMediaTable() {
		return getTable(uniqChecksumUserMediaTable);
	}
	
	public static HTable getLocalUniqChecksumUserMediaTable(){
		return getLocalTable(uniqChecksumUserMediaTable);
	}
	
	public static String getUniqChecksumUserMediaTableName(){
		return Bytes.toString(uniqChecksumUserMediaTable);
	}
	
	public static HTableInterface getTermVectorVersionIDTable() {
		return getTable(termVectorVersionIDTable);
	}
	
	public static HTable getLocalTermVectorVersionIDTable(){
		return getLocalTable(termVectorVersionIDTable);
	}
	
	public static String getTermVectorVersionIDTableName(){
		return Bytes.toString(termVectorVersionIDTable);
	}
	
	public static HTableInterface getDocTable() {
		return getTable(docTable);
	}
	
	public static HTable getLocalDocTable() {
		return getLocalTable(docTable);
	}
	
	public static String getDocTableName(){
		return Bytes.toString(SolbaseUtil.docTable);
	}

    public static HTableInterface getSchemaInfoTable() {
        return getTable(schemaInfoTable);
    }

    public static HTableInterface getDocKeyIdMapTable() {
        return getTable(docKeyIdMapTable);
    }

    public static HTable getLocalDocKeyIdMapTable() {
    	return getLocalTable(docKeyIdMapTable);
    }
    
    public static String getDocKeyIdMapTableName(){
    	return Bytes.toString(SolbaseUtil.docKeyIdMapTable);
    }
    
    public static HTableInterface getUserMediaTable() {
        return getTable(userMediaTable);
    }

    public static HTable getLocalUserMediaTable() {
    	return getLocalTable(userMediaTable);
    }
    
    public static HTableInterface getSequenceTable() {
        return getTable(sequenceTable);
    }
    
    public static String getSequenceTableName(){
    	return Bytes.toString(SolbaseUtil.sequenceTable);
    }
    
    public static void releaseTable(HTableInterface table) {
		hTablePool.putTable(table);
	}

    public static byte[] generateTermKey(Term term) {
		byte[] fieldBytes = Bytes.toBytes(term.field());
		byte[] termBytes = Bytes.toBytes(term.text());
		byte[] fieldTermKeyBytes = Bytes.add(fieldBytes, SolbaseUtil.delimiter,
				termBytes);
		return fieldTermKeyBytes;
	}

	public static byte[] generateTermBeginKey(Term term) {
		return Bytes.add(SolbaseUtil.generateTermKey(term),
				SolbaseUtil.delimiter, SolbaseUtil.floorBytes);
	}

	public static byte[] generateTermEndKey(Term term) {
		return Bytes.add(SolbaseUtil.generateTermKey(term),
				SolbaseUtil.delimiter, SolbaseUtil.delimiter);
	}
	
	public static byte[] generateTermKey(Term term, int startDocId) {
		return Bytes.add(SolbaseUtil.generateTermKey(term),
				SolbaseUtil.delimiter, Bytes.toBytes(startDocId));
	}
	
	public static byte[] getDocumentId(byte[] termDocKey) {
		int maxByteCount = 0;
		int delimiterCount = 0;
		
		for (int i = 0; i < termDocKey.length; i++) {
			if (termDocKey[i] == Byte.MAX_VALUE) {
				maxByteCount++;
			}
			
			if (maxByteCount == 4) {
				delimiterCount++;
				maxByteCount = 0;
			}
			
			if (delimiterCount == 2) {
				return Arrays.copyOfRange(termDocKey, i+1, termDocKey.length);
			}
		}
		
		return null;
	}
	
	public static Integer getDocumentId(ByteBuffer termDocKey) {
		int maxByteCount = 0;
		int delimiterCount = 0;
		
		while (termDocKey.remaining() > 0) {
			byte currentValue = termDocKey.get();
			if (currentValue == Byte.MAX_VALUE) {
				maxByteCount++;
			}
			
			if (maxByteCount == 4) {
				delimiterCount++;
				maxByteCount = 0;
			}
			
			if (delimiterCount == 2) {
				return termDocKey.getInt();
			}
		}
		
		return null;
	}
	
	public static int findDocIdIndex(byte[] termDocKey) {
		int maxByteCount = 0;
		int delimiterCount = 0;
		
		for(int i = 0; i < termDocKey.length; i++){
			byte currentValue = termDocKey[i];
			if (currentValue == Byte.MAX_VALUE) {
				maxByteCount++;
			}
			
			if (maxByteCount == 4) {
				delimiterCount++;
				maxByteCount = 0;
			}
			
			if (delimiterCount == 2) {
				return i + 1;
			}
		}
		
		return -1;
	}
	
    public static int mreadVInt(ByteBuffer buf)
    {       
        int length = buf.remaining();
        
        if(length == 0)
            return 0;
        
        byte b = buf.get();
        int i = b & 0x7F;
        for (int pos = 1, shift = 7; (b & 0x80) != 0 && pos < length; shift += 7, pos++)
        {
            b = buf.get();
            i |= (b & 0x7F) << shift;
        }

        return i;
    }
    
    public static int mreadVInt(InputStream buf) throws IOException
    {               
        byte b = (byte)buf.read();
        int i = b & 0x7F;
        
		for (int shift = 7; (b & 0x80) != 0; shift += 7) {
			b = (byte) buf.read();
			i |= (b & 0x7F) << shift;
		}

        return i;
    }
    
    
    public static byte[] writeVInt(int i)
    {
        int length = 0;
        int p = i;

        while ((p & ~0x7F) != 0)
        {
            p >>>= 7;
            length++;
        }
        length++;

        byte[] buf = new byte[length];
        int pos = 0;
        while ((i & ~0x7F) != 0)
        {
            buf[pos] = ((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
            pos++;
        }
        buf[pos] = (byte) i;

        return buf;
    }
    
    public static Object fromBytes(ByteBuffer data) throws IOException, ClassNotFoundException
    {

        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data.array(), data.position()+data.arrayOffset(), data
                .remaining()));
        Object o = ois.readObject();
        ois.close();
        return o;
    }
    
    public static ByteBuffer toBytes(Object o) throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return ByteBuffer.wrap(baos.toByteArray());
    }

    // sequence doc id mapping to actual silo.picture_id
    public static Integer getDocId(String key) throws IOException {
        HTableInterface docIdKeyMap = SolbaseUtil.getDocKeyIdMapTable();

        try {
            Get get = new Get(Bytes.toBytes(key));
            Result result = docIdKeyMap.get(get);

            if(result.isEmpty()){
                return null;
            }
            
            byte[] docId = result.getValue(Bytes.toBytes("docId"),Bytes.toBytes(""));
            
            int doc = Bytes.toInt(docId); 

            return doc;
        } finally {
            SolbaseUtil.releaseTable(docIdKeyMap);
        }
    }

    // sequence generator for generating doc id
    public static int generateDocId(String key) throws IOException {

        HTableInterface sequence = SolbaseUtil.getSequenceTable();
        HTableInterface docIdKeyMap = SolbaseUtil.getDocKeyIdMapTable();

        try {
            int docId =  new Long(sequence.incrementColumnValue(Bytes.toBytes("sequence"), Bytes.toBytes("id"), Bytes.toBytes(""), 1, true)).intValue();

            
            Put mapping = new Put(Bytes.toBytes(key));
            mapping.add(Bytes.toBytes("docId"), Bytes.toBytes(""), Bytes.toBytes(docId));
            docIdKeyMap.put(mapping);

            return docId;
        } finally {
            SolbaseUtil.releaseTable(sequence);
            SolbaseUtil.releaseTable(docIdKeyMap);
        }
    }
    
    // return uniq id from sequence table
    // mainly used for chunking with pristine indexing
    public static int generateUniqId() throws IOException {
    	HTableInterface sequence = SolbaseUtil.getSequenceTable();
    	
    	try {
    		int docId = new Long(sequence.incrementColumnValue(Bytes.toBytes("sequence"), Bytes.toBytes("id"), Bytes.toBytes(""), SolbaseUtil.UNIQ_ID_CHUNK, true)).intValue();
    	
    		return docId;
    	} finally {
    		SolbaseUtil.releaseTable(sequence);
    	}
    }
    
    public static int getSequenceId(){
    	HTableInterface sequence = SolbaseUtil.getSequenceTable();
    	Get get = new Get(Bytes.toBytes("sequence"));
    	try {
			Result result = sequence.get(get);
			
			if(result == null || result.isEmpty()){
				int docId = new Long(sequence.incrementColumnValue(Bytes.toBytes("sequence"), Bytes.toBytes("id"), Bytes.toBytes(""), 1, true)).intValue();
				return docId;
			} else {
				byte[] val = result.getValue(Bytes.toBytes("id"), Bytes.toBytes(""));
				return new Long(Bytes.toLong(val)).intValue();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
    }
    
    public static int getCurrentMaxId(){
    	return getSequenceId();
    }
    
    public static int getEpochSinceSolbase(long currentMinutes){
    	return (int)(currentMinutes - SolbaseEpochTime);
    }
    
    public static long getCurrentTimeFromEpochSinceSolbase(String currentMinutes){
    	int currentMin = 0;
    	try {
    		currentMin = Integer.parseInt(currentMinutes);
    		return (SolbaseEpochTime + currentMin) * 60;
    	} catch (NumberFormatException e) {
    		// ignore
    	}
    	return currentMin;
    }
    
	public static void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, Integer numberOfRegions) throws IOException{
		HBaseAdmin admin = new HBaseAdmin(SolbaseUtil.conf);

		if(startKey != null && endKey != null && numberOfRegions != null){
			admin.createTable(desc, startKey, endKey, numberOfRegions);
		} else {
			admin.createTable(desc);
		}
	}
	
	public static void setupHColumnDescriptor(HColumnDescriptor column){
		column.setCompressionType(Algorithm.SNAPPY);
		column.setScope(1);
		column.setMaxVersions(1);
	}
	
	public static void createTermVectorTable(byte[] startTerm, byte[] endTerm, Integer numberOfRegions) throws IOException {
		HTableDescriptor desc = new HTableDescriptor(SolbaseUtil.getTermVectorTableName());
		HColumnDescriptor column = new HColumnDescriptor(SolbaseUtil.termVectorDocColumnFamilyName);
		SolbaseUtil.setupHColumnDescriptor(column);
		desc.addFamily(column);
		SolbaseUtil.createTable(desc, startTerm, endTerm, numberOfRegions);
	}
    
	public static void createTermVectorVersionIDTable() throws IOException {
		HTableDescriptor desc = new HTableDescriptor(SolbaseUtil.getTermVectorVersionIDTableName());
		HColumnDescriptor column = new HColumnDescriptor(SolbaseUtil.timestampColumnFamilyName);
		SolbaseUtil.setupHColumnDescriptor(column);
		desc.addFamily(column);
		SolbaseUtil.createTable(desc,null,null,null);
	}

	public static void createDocKeyIdMapTable(byte [] start, byte[] end, Integer numberOfRegions) throws IOException {
		HTableDescriptor desc = new HTableDescriptor(SolbaseUtil.getDocKeyIdMapTableName());
		HColumnDescriptor column = new HColumnDescriptor(SolbaseUtil.docIdColumnFamilyName);
		SolbaseUtil.setupHColumnDescriptor(column);
		desc.addFamily(column);
		SolbaseUtil.createTable(desc, start, end, numberOfRegions);		
	}

	public static void createDocTable(byte[] start, byte[] end, Integer numberOfRegions) throws IOException {
		HTableDescriptor desc = new HTableDescriptor(SolbaseUtil.getDocTableName());
		HColumnDescriptor fieldColumn = new HColumnDescriptor(SolbaseUtil.fieldColumnFamilyName);
		SolbaseUtil.setupHColumnDescriptor(fieldColumn);
		desc.addFamily(fieldColumn);
		
		HColumnDescriptor allTermsColumn = new HColumnDescriptor(SolbaseUtil.allTermsColumnFamilyName);
		SolbaseUtil.setupHColumnDescriptor(allTermsColumn);
		desc.addFamily(allTermsColumn);
		
		HColumnDescriptor timestampColumn = new HColumnDescriptor(SolbaseUtil.timestampColumnFamilyName);
		SolbaseUtil.setupHColumnDescriptor(timestampColumn);
		desc.addFamily(timestampColumn);
		
		SolbaseUtil.createTable(desc, start, end, numberOfRegions);	
	}

	public static void createSequenceTable() throws IOException {
		HTableDescriptor desc = new HTableDescriptor(SolbaseUtil.getSequenceTableName());
		HColumnDescriptor column = new HColumnDescriptor(SolbaseUtil.idColumnFamilyName);
		SolbaseUtil.setupHColumnDescriptor(column);
		desc.addFamily(column);
		SolbaseUtil.createTable(desc, null, null, null);
	}

	public static void createUniqChecksumUserMediaTable(byte[] start, byte[] end, Integer numberOfRegions) throws IOException {
		HTableDescriptor desc = new HTableDescriptor(SolbaseUtil.getUniqChecksumUserMediaTableName());
		HColumnDescriptor column = new HColumnDescriptor(SolbaseUtil.userMediaKeyColumnFamilyName);
		SolbaseUtil.setupHColumnDescriptor(column);
		desc.addFamily(column);
		SolbaseUtil.createTable(desc, start, end, numberOfRegions);
	}
	
	public static byte[] randomize(Integer docId){
		byte[] bytes = Bytes.toBytes(docId);
		ArrayUtils.reverse(bytes);
		return bytes;
	}
	
	public static byte[] randomize(byte[] docId){
		ArrayUtils.reverse(docId);
		return docId;
	}

	public static void main(String[] args){
		try {
			@SuppressWarnings("deprecation")
			HBaseConfiguration conf = new HBaseConfiguration();
			conf.set("hbase.zookeeper.quorum", "den3dhdptk01");
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			conf.setInt("hbase.client.retries.number", 7);
			conf.setInt("ipc.client.connect.max.retries", 3);

			int docNumber = -1;
			
			HTableInterface htable = hTablePool.getTable("DocKeyIdMap_activity");

			long totalTime = 0;
			long totalTerms = 0;
			// using seperate connector to leverage different http thread
			// pool for updates
			CommonsHttpSolrServer solbaseServer = new CommonsHttpSolrServer("http://den3devsch04:8080/solbase/activity~0");

			long totalStart = System.currentTimeMillis();
			int j = 0;
			int k = 0;
			
			for(; k < 100; k++){
				String globalId = "target:id:" + k;

				SolrInputDocument doc = new SolrInputDocument();
				
				try {
					Get docIdGet = new Get(Bytes.toBytes(globalId));
					Result result = htable.get(docIdGet);

					if (result != null && !result.isEmpty()) {
						byte[] docIdBytes = result.getValue(Bytes.toBytes("docId"), Bytes.toBytes(""));
						docNumber = Bytes.toInt(docIdBytes);
						doc.addField("edit", true);
					} else {
						HTableInterface sequence = hTablePool.getTable("Sequence_activity");

						try {
							docNumber = new Long(sequence.incrementColumnValue(Bytes.toBytes("sequence"), Bytes.toBytes("id"), Bytes.toBytes(""), 1, true)).intValue();
						} catch (IOException e) {
							e.printStackTrace();
						} finally {
							hTablePool.putTable(sequence);
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					hTablePool.putTable(htable);
				}

				doc.addField("docId", docNumber);
				doc.addField("global_uniq_id", globalId);
				doc.addField("updateStore", true);

				// time
				long start = System.currentTimeMillis();

				String targetIds = "";
				
				int randomNumTerms = (int) (50 * Math.random());
				randomNumTerms = 100;
				totalTerms += randomNumTerms;
				
				for (j=0; j < randomNumTerms; j++) {
					targetIds += " " + j;
				}
				doc.addField("targetIds", targetIds);
				doc.addField("lastModified", new Integer(SolbaseUtil.getEpochSinceSolbase((System.currentTimeMillis() - k*60000) / 60000)).toString());

				solbaseServer.add(doc);

				doc.removeField("targetIds");
				doc.removeField("lastModified");

				long elapsed = System.currentTimeMillis() - start;

				System.out.println("time to update " + j + " terms: " + elapsed);

				totalTime += elapsed;
			}
			
			System.out.println("average time to update " + totalTerms/k + " terms: " + totalTime/k);
			System.out.println("updated: " + k + " docs in " + (System.currentTimeMillis() - totalStart));
		} catch (MalformedURLException e) {

		} catch (SolrServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
