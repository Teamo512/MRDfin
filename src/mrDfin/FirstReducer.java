package mrDfin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;


public class FirstReducer extends Reducer<IntWritable, IntWritable, IntWritable, Writable>{
	private int minSupport;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException{
		minSupport = context.getConfiguration().getInt("minSup", 0);

	}
	
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		int sum = 0;
		
		for(IntWritable value : values){
			sum += value.get();
		}
		if(sum >= minSupport){
			context.write(key, new IntWritable(sum));
			context.getCounter(MRDfinCounter.TatolFrequentNum).increment(1);
		}
	}
}
