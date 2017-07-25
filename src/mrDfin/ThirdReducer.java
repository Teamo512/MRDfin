package mrDfin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ThirdReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
	
	public void reduce(LongWritable key, Iterable<LongWritable> values, Context context){
		
	}

}
