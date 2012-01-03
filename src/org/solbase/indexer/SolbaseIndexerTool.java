package org.solbase.indexer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.solbase.SolbaseUtil;
import org.solbase.indexer.mapreduce.SolbaseIndexReducer;

// example scan query to get term vector rows - this is actual search query given term. term in this example query is "logan"
//scan 'TV', {STARTROW=>"contents\x7F\x7F\x7F\x7Flogan\x7F\x7F\x7F\x7F\x00\x00\x00\x00", ENDROW=>"contents\x7F\x7F\x7F\x7Flogan\x7F\x7F\x7F\x7F\x7F\x7F\x7F\x7F"}

public class SolbaseIndexerTool implements Tool{
	private Configuration conf;
	
	private SolbaseIndexUtil indexerUtil; 
	
	public SolbaseIndexerTool(SolbaseIndexUtil util){
		this.indexerUtil = util;
	}
	
    public String convertScanToString(Scan scan) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        scan.write(dos);
        return Base64.encodeBytes(out.toByteArray());
    }
    
	@Override
	public Configuration getConf() {
		return this.conf;
	}
	
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		if(this.conf == null){
			this.conf = new Configuration();
		}
		
		String tableName = arg0[0];
		
		// for debugging, this will run local
		// in 0.90, this only works for mapping phase, after that, it tries to copy blocks to reduce and never finishes that phase
		//conf.set("mapred.job.tracker", "local");

		// set up tables ahead of MR job
		setupTables();
		
		Scan scan = indexerUtil.getScanner();
	    
	    // tablemapreduceutil way of doing it
	    Job job = new Job(conf);
	    // used to work fine without explicit resource addition of hbase to map/red in .89
	    job.getConfiguration().addResource("hbase-site.xml");
	    
	    job.getConfiguration().set("indexerUtil", indexerUtil.getClass().getName());
	    
		job.setJarByClass(org.solbase.indexer.mapreduce.SolbaseInitialIndexMapper.class);
		TableMapReduceUtil.initTableMapperJob(tableName, scan, org.solbase.indexer.mapreduce.SolbaseInitialIndexMapper.class, BytesWritable.class, MapWritable.class, job);

	    job.setJarByClass(org.solbase.indexer.mapreduce.SolbaseIndexReducer.class);
	    job.setJarByClass(org.solbase.lucenehbase.TermDocMetadata.class);
	    job.setJarByClass(org.solbase.lucenehbase.IndexWriter.class);
	    job.setJarByClass(org.solbase.SolbaseUtil.class);
	    job.setJarByClass(org.solbase.indexer.writable.TermDocMetadataWritable.class);
	    job.setJarByClass(org.solbase.indexer.writable.DocumentPutWritable.class);
	    job.setJarByClass(org.apache.lucene.document.EmbeddedSortField.class);

	    job.setOutputKeyClass(ImmutableBytesWritable.class);
	    job.setOutputValueClass(MapWritable.class);
	    job.setReducerClass(SolbaseIndexReducer.class);
	    job.setOutputFormatClass(MultiTableOutputFormat.class);
	    
	    // 1.75 * number of node (6) * max tasks (4) = 42, but seems small
	    job.setNumReduceTasks(48);
	    job.waitForCompletion(true);

		return 0;
	}

	private void setupTables() {
		HBaseAdmin admin;
		try {
			// should be running on the cluster that has zoo.cfg or hbase-site.xml on hadoop/hbase classpath already.
			Configuration conf = HBaseConfiguration.create();
			admin = new HBaseAdmin(conf);

			if (!admin.isTableAvailable(SolbaseUtil.termVectorTable)) {				
				SolbaseUtil.createTermVectorTable(null, null, null);
			} else {
				//SolbaseUtil.truncateTable(admin, SolbaseUtil.termVectorTable);
			}
			if (!admin.isTableAvailable(SolbaseUtil.termVectorVersionIDTable)) {
				SolbaseUtil.createTermVectorVersionIDTable();
			} else {
				//SolbaseUtil.truncateTable(admin, SolbaseUtil.termVectorVersionIDTable);
			}
			if (!admin.isTableAvailable(SolbaseUtil.docKeyIdMapTable)) {
				SolbaseUtil.createDocKeyIdMapTable(null, null, null);
			} else {
				//SolbaseUtil.truncateTable(admin, SolbaseUtil.docKeyIdMapTable);
			}
			if (!admin.isTableAvailable(SolbaseUtil.docTable)) {
				SolbaseUtil.createDocTable(null, null, null);
			} else {
				//SolbaseUtil.truncateTable(admin, SolbaseUtil.docTable);
			}
			if (!admin.isTableAvailable(SolbaseUtil.sequenceTable)) {
				SolbaseUtil.createSequenceTable();
			} else {
				//SolbaseUtil.truncateTable(admin, SolbaseUtil.sequenceTable);
			}
			if (!admin.isTableAvailable(SolbaseUtil.uniqChecksumUserMediaTable)) {
				SolbaseUtil.createUniqChecksumUserMediaTable(null, null, null);
			} else {
				//SolbaseUtil.truncateTable(admin, SolbaseUtil.uniqChecksumUserMediaTable);
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
