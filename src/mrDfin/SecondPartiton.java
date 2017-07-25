package mrDfin;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SecondPartiton extends HashPartitioner<KeyWritable, ValueWritable>{

	@Override
    public int getPartition(KeyWritable key, ValueWritable value, int numReduceTasks) {
		return (key.groupNum & Integer.MAX_VALUE) % numReduceTasks;
    }
}
