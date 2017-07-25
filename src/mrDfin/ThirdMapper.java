package mrDfin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ThirdMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{

	
	public void map(LongWritable key, LongWritable value, Context context){
		
	}
}
