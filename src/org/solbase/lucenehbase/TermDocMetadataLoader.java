package org.solbase.lucenehbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.ResourceBundle;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.index.Term;
import org.solbase.SolbaseShardUtil;
import org.solbase.SolbaseUtil;
import org.solbase.cache.CachedObjectLoader;
import org.solbase.cache.CachedObjectWrapper;
import org.solbase.cache.LayeredCache;
import org.solbase.cache.LayeredCache.ModificationType;

public class TermDocMetadataLoader implements CachedObjectLoader<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier, TermDocMetadata> {

	public static final int CHUNK_SIZE = 1000 * 100;
	private final static Logger logger = Logger.getLogger(TermDocMetadataLoader.class);

	public enum STORAGE_TYPE {
		KEY_ONLY, WIDE_ROW, NARROW_ROW;
	}

	public static STORAGE_TYPE storageType;
	static {
		String type = System.getProperty("solbase.storage.type");
		if (type == null && ResourceBundle.getBundle("solbase") != null) {
			try {
				type = ResourceBundle.getBundle("solbase").getString("storage.type");
			} catch (java.util.MissingResourceException ex) {

			}
		}
		if (type == null || type.isEmpty()) {
			storageType = STORAGE_TYPE.NARROW_ROW;
		} else {
			storageType = STORAGE_TYPE.valueOf(type);
		}
	}

	public CachedObjectWrapper<CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier> loadObject(Term term, int start, int end, LayeredCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier, TermDocMetadata> cache) throws IOException {
		HTableInterface termVectorTable = SolbaseUtil.getTermVectorTable();
		try {

			byte[] termBeginKey = SolbaseUtil.generateTermKey(term, start);
			byte[] termEndKey;
			if(end > SolbaseShardUtil.getMaxDocId()){
				// in case, we are in last shard, our end key is always get start from \xff\xff\xff\xff
				// meaning fetch remaining docs.
				termEndKey = SolbaseUtil.generateTermEndKey(term);	
			} else {
				termEndKey = SolbaseUtil.generateTermKey(term, end);
			}
			
			Scan fieldScan = new Scan(termBeginKey, termEndKey);
			fieldScan.addFamily(SolbaseUtil.termVectorDocColumnFamilyName);
			fieldScan.setBatch(2000);
			fieldScan.setCaching(2000);
			ResultScanner fieldScanner = termVectorTable.getScanner(fieldScan);

			Result termDoc;

			ByteArrayOutputStream bis = new ByteArrayOutputStream();
			int docAmount = 0;

			while ((termDoc = fieldScanner.next()) != null) {
				if (storageType == STORAGE_TYPE.WIDE_ROW) {
					convertResultChunkToTermDoc(termDoc, bis);
				} else {
					convertResultToTermDoc(termDoc, bis);
					docAmount++;
				}
			}

			fieldScanner.close();

			logger.info("Read from HBase for term: " + term.toString() + " has this many docs: " + docAmount);
			
			// TODO LOAD VERSION
			TermDocMetadataVersionIdentifier versionIdentifier = getVersionIdentifier(term, start, end);
			return new CachedObjectWrapper<CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier>(new CompactedTermDocMetadataArray(bis, docAmount), versionIdentifier, System.currentTimeMillis());

		} finally {
			SolbaseUtil.releaseTable(termVectorTable);
		}
	}

	private void convertResultChunkToTermDoc(Result termDocQueryResult, ByteArrayOutputStream bis) throws IOException {
		NavigableMap<byte[], byte[]> columns = termDocQueryResult.getFamilyMap(SolbaseUtil.termVectorDocColumnFamilyName);
		for (Entry<byte[], byte[]> column : columns.entrySet()) {
			byte[] serializedTermDocMetadata = column.getValue();
			byte[] docId = column.getKey();

			bis.write(docId);
			bis.write(serializedTermDocMetadata);
		}
	}

	private void convertResultToTermDoc(Result termDocQueryResult, ByteArrayOutputStream bis) throws IOException {
		switch (storageType) {
		case KEY_ONLY: {
			byte[] row = termDocQueryResult.getRow();
			int start = SolbaseUtil.findDocIdIndex(row);
			bis.write(row, start, row.length - start);
		}
			break;
		case WIDE_ROW:
			break;
		case NARROW_ROW:
		default: {
			throw new UnsupportedOperationException();
		}
		}
	}
	
	public static TermDocMetadataVersionIdentifier getStaticVersionIdentifier(Term key, int startDocId, int endDocId) throws IOException {
		HTableInterface termVectorVersionIDTable = SolbaseUtil.getTermVectorVersionIDTable();

		try {
			byte[] fieldTermKey = SolbaseUtil.generateTermKey(key);

			Get get = new Get(Bytes.add(fieldTermKey, Bytes.toBytes(startDocId), Bytes.toBytes(endDocId)));
			Result result = termVectorVersionIDTable.get(get);

			if (result.isEmpty()) {
				Put updatePut = new Put(Bytes.add(fieldTermKey, Bytes.toBytes(startDocId), Bytes.toBytes(endDocId)));
				long currentTime = System.currentTimeMillis();
				updatePut.add(SolbaseUtil.timestampColumnFamilyName, Bytes.toBytes(""), Bytes.toBytes(currentTime));
				termVectorVersionIDTable.put(updatePut);
				
				return new TermDocMetadataVersionIdentifier(currentTime, startDocId, endDocId);
			} else {

				return new TermDocMetadataVersionIdentifier(Bytes.toLong(result.getValue(Bytes.toBytes("timestamp"), Bytes.toBytes(""))), startDocId, endDocId);
			}

		} finally {
			SolbaseUtil.releaseTable(termVectorVersionIDTable);
		}
		
	}

	public TermDocMetadataVersionIdentifier getVersionIdentifier(Term key, int startDocId, int endDocId) throws IOException {
		return TermDocMetadataLoader.getStaticVersionIdentifier(key, startDocId, endDocId);
	}

	public static Integer getChunkId(int docId) {
		return (docId / TermDocMetadataLoader.CHUNK_SIZE) * TermDocMetadataLoader.CHUNK_SIZE;
	}

	public static void main(String[] argv) {
		System.out.println((99999 / TermDocMetadataLoader.CHUNK_SIZE) * TermDocMetadataLoader.CHUNK_SIZE);
		System.exit(0);
	}

	@Override
	public void updateObject(CachedObjectWrapper<CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier> object, TermDocMetadata modificationdData, LayeredCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier, TermDocMetadata> cache, LayeredCache.ModificationType modType, int startDocId, int endDocId) throws IOException {
		int docId = modificationdData.getDocId();
		CompactedTermDocMetadataArray ctdma = object.getValue();

		if (modType == ModificationType.DELETE) {
			// deleting term vector from term vector byte array
			if (ctdma != null) {
				ctdma.readWriteLock.writeLock().lock();
				try {
					int docAmount = ctdma.getDocAmount();

					SolbaseTermDocs std = new SolbaseTermDocs(ctdma);

					// offset into byte array
					int prevPosition = std.termDocs.currentPostion();

					// iterate over term vector array until current doc is greater than docId we want to delete
					while (std.next() && std.doc() < docId) {
						prevPosition = std.termDocs.currentPostion();
					}

					// if current docid matches docid we want to delete, properly delete term vector out of array
					if (std.doc() == docId) {
						int currentPosition = std.termDocs.currentPostion();
						
						docAmount--;
						ctdma.setDocAmount(docAmount);
						ctdma.deleteTermVector(prevPosition, currentPosition);
					}
					
					Term term = modificationdData.getTerm();

					logger.debug("term delete: " + term.toString() + " has this many docs: " + docAmount + " docId: " + docId);

					TermDocMetadataVersionIdentifier newVersionId = new TermDocMetadataVersionIdentifier(System.currentTimeMillis(), startDocId, endDocId);
					object.setVersionIdentifier(newVersionId);
					modificationdData.versionIdentifier = newVersionId;
				} finally {
					ctdma.readWriteLock.writeLock().unlock();
				}
			}
		} else {
			Term term = modificationdData.getTerm();
			logger.debug("entering tv updateObject for " + term.toString() + " docId: " + docId);
			if (ctdma != null) {
				ctdma.readWriteLock.writeLock().lock();
				try {
					int docAmount = ctdma.getDocAmount();

					byte[] byteArray = ctdma.getTermDocMetadataArray();
					SolbaseTermDocs std = new SolbaseTermDocs(ctdma);

					int prevPosition = std.termDocs.currentPostion();

					while (std.next() && std.doc() < docId) {
						prevPosition = std.termDocs.currentPostion();
					}

					byte[] newTdm = Bytes.add(Bytes.toBytes(docId), Bytes.toBytes(modificationdData.serialize()));
					
					if (std.doc() == docId) {	
						// UPDATING EXISTING DOC
						int currentPosition = std.termDocs.currentPostion();
						int prevTermDocSize = currentPosition - prevPosition;
						// if we can fit new term doc into exisitng space, do it!
						if(newTdm.length < prevTermDocSize + ctdma.bufferedSize()){
							ctdma.updateTermVector(prevPosition, currentPosition, newTdm);
						} else {
							// new term doc metadata won't fit in buffered space, so copy it over and create new size
							ByteArrayOutputStream baos = new ByteArrayOutputStream(byteArray.length + newTdm.length);
							baos.write(byteArray, 0, prevPosition);
							baos.write(newTdm);
							baos.write(byteArray, currentPosition, byteArray.length - currentPosition);			
							
							// calculate new buffer size and fill 0's
							int origSize = baos.size();
							int bufferedSize = CompactedTermDocMetadataArray.bufferTermVectorArray(baos);
							baos.write(new byte[bufferedSize - origSize], 0, bufferedSize - origSize);
							
							byteArray = baos.toByteArray();

							ctdma.setTermDocMetadataArray(byteArray);
							ctdma.setTermVectorSize(origSize);
						}
					} else {
						// ADDING NEW DOC
						if (newTdm.length < ctdma.bufferedSize()) {
							docAmount++;
							int currentPosition = std.termDocs.currentPostion();
							
							ctdma.setDocAmount(docAmount);
							ctdma.addTermVector(prevPosition, currentPosition, newTdm);

 						} else {
							// new term doc metadata won't fit in buffered space, so copy it over and create new size
 							int termVectorSize = ctdma.getTermVectorSize();
							ByteArrayOutputStream baos = new ByteArrayOutputStream(termVectorSize + newTdm.length);
							baos.write(byteArray, 0, prevPosition);
							baos.write(newTdm);
							baos.write(byteArray, prevPosition, termVectorSize - prevPosition);
							docAmount++;	
							
							// calculate new buffer size and fill 0's
							int origSize = baos.size();
							int bufferedSize = CompactedTermDocMetadataArray.bufferTermVectorArray(baos);
							baos.write(new byte[bufferedSize - origSize], 0, bufferedSize - origSize);
							
							byteArray = baos.toByteArray();

							ctdma.setDocAmount(docAmount);
							ctdma.setTermDocMetadataArray(byteArray);
							ctdma.setTermVectorSize(origSize);
						}
					}

					logger.debug("term update: " + term.toString() + " has this many docs: " + docAmount + " docId: " + docId);
					
					TermDocMetadataVersionIdentifier newVersionId = new TermDocMetadataVersionIdentifier(System.currentTimeMillis(), startDocId, endDocId);
					object.setVersionIdentifier(newVersionId);
					modificationdData.versionIdentifier = newVersionId;
				} finally {
					ctdma.readWriteLock.writeLock().unlock();
				}
			}
		}

	}

	@Override
	public void updateObjectStore(Term key, TermDocMetadata modificationData, IndexWriter writer, LayeredCache<Term, CompactedTermDocMetadataArray, TermDocMetadataVersionIdentifier, TermDocMetadata> cache, LayeredCache.ModificationType modType, int startDocId, int endDocId) throws IOException {
		logger.debug("update store docId: " + modificationData.docId + " term: " + key.toString());
		if(modType == LayeredCache.ModificationType.DELETE){
			// this is delete
			writer.deleteTermVector(modificationData, startDocId, endDocId);
			writer.updateTermVectorVersionId(modificationData, startDocId, endDocId);
		} else if(modType == LayeredCache.ModificationType.UPDATE){
			writer.updateTermVector(modificationData, startDocId, endDocId);
			writer.updateTermVectorVersionId(modificationData, startDocId, endDocId);
		} else if(modType == LayeredCache.ModificationType.ADD){
			writer.addTermVector(modificationData);
			writer.updateTermVectorVersionId(modificationData, startDocId, endDocId);
		}
		
	}

}
