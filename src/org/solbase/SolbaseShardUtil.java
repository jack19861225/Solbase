
package org.solbase;

import java.util.ArrayList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.solr.common.util.StrUtils;

public class SolbaseShardUtil {
	
	// do we want to set max doc per shard or have constant number of shard and have 
	//public static final int MAX_PER_SHARD = 1000 * 1000 * 10; // 10 million docs per shard

	private static int MAX_DOC_ID;
	private static int NUM_PER_SHARD;
	private static int NUM_DOC_PADDING = 1000 * 1000; //1000 * 1000; // 1 million for now. we need to pad each shard for real time indexing. this kinda sucks cause last shard will always have less data assigned
	private static List<String> SHARD_HOSTS;
	private static int NUM_SHARDS;
		
	static {
		ResourceBundle resources = ResourceBundle.getBundle("solbase");
		if (resources != null) {
			try {
				String shards = System.getProperty("solbase.shard.hosts");
				if(shards == null && ResourceBundle.getBundle("solbase") != null){
					shards = resources.getString("shard.hosts");
				}			

				SHARD_HOSTS = StrUtils.splitSmart(shards, ",", true);
				for (int i = 0; i < SHARD_HOSTS.size(); i++) {
					SHARD_HOSTS.set(i, SHARD_HOSTS.get(i) + ":8080");
				}
				NUM_SHARDS = SHARD_HOSTS.size();
			} catch (MissingResourceException e) {
				NUM_SHARDS = 0;
				SHARD_HOSTS = new ArrayList<String>();
				SHARD_HOSTS.add("localhost:8080");
			}
		} else {
			NUM_SHARDS = 0;
			SHARD_HOSTS = new ArrayList<String>();
			SHARD_HOSTS.add("localhost:8080");
		}

		// this number is current max doc id from table.
		MAX_DOC_ID = SolbaseUtil.getCurrentMaxId();
		// given current max doc id, divide that by number of shard and add
		// padding to the end
		if(NUM_SHARDS == 0){
			NUM_PER_SHARD = MAX_DOC_ID + NUM_DOC_PADDING;
		} else {
			NUM_PER_SHARD = (MAX_DOC_ID / NUM_SHARDS) + NUM_DOC_PADDING;
		}
	}
	
	public static List<String> getShardHosts(){
		return SHARD_HOSTS;
	}
	
    public static int getNumShard(){
    	return NUM_SHARDS;
    }
    
    public static int getStartDocId(int shardNum){
    	return shardNum * NUM_PER_SHARD;
    }
    
    public static int getEndDocId(int startDocId){
    	return startDocId + NUM_PER_SHARD - 1;
    }
    
	public static int getShardNum(String indexName) {
		int in = indexName.indexOf("~");

		if (in >= 0) {
			String num = indexName.substring(in + 1);
			int shardNum = Integer.parseInt(num);
			return shardNum;
		} else {
			return 0;
		}
	}
	public static int getNumPerShard(){
		return NUM_PER_SHARD;
	}
	
	public static int getMaxDocId(){
		// note: this isn't current max doc id. at the start of solbase, max id
		return MAX_DOC_ID;
	}
}
