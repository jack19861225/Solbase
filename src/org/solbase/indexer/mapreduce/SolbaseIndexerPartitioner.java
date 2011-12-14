package org.solbase.indexer.mapreduce;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SolbaseIndexerPartitioner extends Partitioner<BytesWritable, MapWritable>{

	private static Integer chunk = 1000 * 1000; //1000 * 1000 * 20;
	
	@Override
	public int getPartition(BytesWritable key, MapWritable value, int numPartitions) {

		if(value.containsKey(new BytesWritable(Bytes.toBytes("doc")))){
			int docId = Bytes.toInt(key.getBytes());
			return docId/chunk % numPartitions;
		} else {
			HashPartitioner<BytesWritable, MapWritable> partitioner = new HashPartitioner<BytesWritable, MapWritable>();
			return partitioner.getPartition(key, value, numPartitions);
		}
	}
}
