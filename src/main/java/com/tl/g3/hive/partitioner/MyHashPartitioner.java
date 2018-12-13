package com.tl.g3.hive.partitioner;

import org.apache.hadoop.hive.ql.io.HivePartitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;

public class MyHashPartitioner<K2, V2> extends HashPartitioner<K2, V2>
		implements HivePartitioner<K2, V2> {
	@Override
	public int getPartition(K2 key, V2 value, int numReduceTasks) {
		return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}

	@Override
	public int getBucket(K2 key, V2 value, int numBuckets) {
		return 0;
	}

}
