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
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MultiTableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
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
		// for debugging, this will run local
		// in 0.90, this only works for mapping phase, after that, it tries to copy blocks to reduce and never finishes that phase
		//conf.set("mapred.job.tracker", "local");

		// set up tables ahead of MR job
		setupTables();
		
		// single regions
		Scan scan = new Scan();
		
		FilterList filters = new FilterList();
		// fetch public pictures only
		SingleColumnValueFilter publicFilter = new SingleColumnValueFilter(Bytes.toBytes("meta"), Bytes.toBytes("public"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1"));
		// fetch non tombstoned only
		SingleColumnValueFilter tombstoneFilter = new SingleColumnValueFilter(Bytes.toBytes("meta"), Bytes.toBytes("tombstoned"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("0"));
	    filters.addFilter(publicFilter);
	    filters.addFilter(tombstoneFilter);
		scan.setFilter(filters);
	
		scan.addFamily(Bytes.toBytes("meta"));
		scan.addFamily(Bytes.toBytes("tags"));
	    scan.addFamily(Bytes.toBytes("stats"));
	    scan.setBatch(1000);
	    scan.setCaching(1000);
	    
	    // tablemapreduceutil way of doing it
	    Job job = new Job(conf);
	    // used to work fine without explicit resource addition of hbase to map/red in .89
	    job.getConfiguration().addResource("hbase-site.xml");
	    
	    job.getConfiguration().set("indexerUtil", indexerUtil.getClass().getName());
	    
		job.setJarByClass(org.solbase.indexer.mapreduce.SolbaseInitialIndexMapper.class);
		TableMapReduceUtil.initTableMapperJob("user_media", scan, org.solbase.indexer.mapreduce.SolbaseInitialIndexMapper.class, BytesWritable.class, MapWritable.class, job);

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

			// TODO: implement pre split - key distribution logic
			if (!admin.isTableAvailable(SolbaseUtil.termVectorTable)) {				
				/*
				String startTerm = indexerUtil.getStartTerm();
				String endTerm = indexerUtil.getEndTerm();

				if(startTerm != null && endTerm != null){
					byte[] start = SolbaseUtil.generateTermBeginKey(new Term(startTerm));
					byte[] end = SolbaseUtil.generateTermEndKey(new Term(endTerm));
					SolbaseUtil.createTermVectorTable(start, end, indexerUtil.getNumberOfTVRegions());
				} else {
				*/
					SolbaseUtil.createTermVectorTable(null, null, null);
				//}
			}
			if (!admin.isTableAvailable(SolbaseUtil.termVectorVersionIDTable)) {
				SolbaseUtil.createTermVectorVersionIDTable();
			}
			if (!admin.isTableAvailable(SolbaseUtil.docKeyIdMapTable)) {
				/*
				byte[] start = indexerUtil.getStartDocKey();
				byte[] end = indexerUtil.getEndDocKey();

				if (start != null && end != null) {
					SolbaseUtil.createDocKeyIdMapTable(start, end, indexerUtil.getNumberOfDocKeyRegions());
				} else {
				 */
					SolbaseUtil.createDocKeyIdMapTable(null, null, null);	
				//}
			}
			if (!admin.isTableAvailable(SolbaseUtil.docTable)) {
				/*
				Integer maxDocs = indexerUtil.getMaxDocs();
				if (maxDocs != null) {
					int regions = indexerUtil.getNumberOfDocRegions();
					int perRegion = maxDocs / regions;
					SolbaseUtil.createDocTable(Bytes.toBytes(perRegion), Bytes.toBytes(perRegion*(regions-1)), regions);
				} else {
					*/
					SolbaseUtil.createDocTable(null, null, null);
				//}
			}
			if (!admin.isTableAvailable(SolbaseUtil.sequenceTable)) {
				SolbaseUtil.createSequenceTable();
			}
			if (!admin.isTableAvailable(SolbaseUtil.uniqChecksumUserMediaTable)) {
				/*
				String startUniqKey = indexerUtil.getUniqStartKey();
				String endUniqKey = indexerUtil.getUniqEndKey();

				if (startUniqKey != null && endUniqKey != null) {
					SolbaseUtil.createUniqChecksumUserMediaTable(Bytes.toBytes(startUniqKey), Bytes.toBytes(endUniqKey), indexerUtil.getNumberOfUniqRegions());
				} else {
					*/
					SolbaseUtil.createUniqChecksumUserMediaTable(null, null, null);
				//}
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
