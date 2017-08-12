package mrDfin;

import static mrDfin.MRDfin_Driver.ItemDelimiter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * First MRJob, just like wordcount
 * set a HashMaP as the cache structure
 * @author jiang
 *
 */
public class FirstMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

	IntWritable one = new IntWritable(1);
	HashMap<Integer, Integer> hashmap = new HashMap<Integer, Integer>();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] line = value.toString().split(ItemDelimiter);
		for(int i=1; i<line.length; i++){
			int item = Integer.parseInt(line[i]);
			Integer count = hashmap.get(item);
			if(count == null){
				hashmap.put(item, 1);
			}else{
				hashmap.put(item, ++count);
			}
		}
	}
	
	/*public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
		IntWritable oneValue = new IntWritable(1);
		String[] line = value.toString().split(" ");
		for(int i = 0; i < line.length; i++){
			IntWritable item = new IntWritable(Integer.parseInt(line[i]));
			context.write(item, oneValue);
		}
		
	}*/
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		if(!hashmap.isEmpty()){
			for(Map.Entry<Integer, Integer> entry : hashmap.entrySet()){
				context.write(new IntWritable(entry.getKey()), new IntWritable(entry.getValue()));
				
			}
		}
	}

}
