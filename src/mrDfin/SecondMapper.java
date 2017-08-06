package mrDfin;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondMapper extends Mapper<Object, Text, IntWritable, ValueWritable>{

	private Map<Integer, Integer> allItemsMap; 
	private Map<Integer, Integer> itemGroupNum; 
	private int groupsNum;
	/**
	 * 获取缓存文件，因为要保证最终的flist是按支持度降序排列的，所以此处将文件的迭代访问读取工作放在了getFilst函数中
	 */
	protected void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);
		allItemsMap = new HashMap<Integer, Integer>();
		itemGroupNum = new HashMap<Integer, Integer>();
		groupsNum = context.getConfiguration().getInt("GroupsNum", 1);
		
		boolean useFileCache = context.getConfiguration().getBoolean("Cache", true);
		
		if(useFileCache)
			getItem(context);
		else
			getItem_bak(context);
		/*URI[] paths = context.getCacheFiles();
		if(paths == null || paths.length <=0){
			System.out.println("No DistributedCache keywords File!");
			System.exit(1);
		}
		SequenceFile.Reader reader = null;
		try {
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			for(URI path : paths){	
				if(path.getPath().contains("Flist")) {
					reader = new SequenceFile.Reader(context.getConfiguration(), Reader.file(new Path(path)));
					while (reader.next(key, value)) {
						allItemsMap.put(key.get(), value.get());
					}
				}else if(path.getPath().contains("groupNum")) {
					reader = new SequenceFile.Reader(context.getConfiguration(), Reader.file(new Path(path)));
					while (reader.next(key, value)) {
						itemGroupNum.put(key.get(), value.get());
					}
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			IOUtils.closeStream(reader);
		}*/
		
		
		/*String flist = context.getConfiguration().get("Flist");
		int i = 0;
		for(String str : flist.split(" ")) {
			allItemsMap.put(Integer.parseInt(str), i++);
		}
		String[] str = context.getConfiguration().get("ItemGroup").split(";");
		for(String s : str) {
			String[] ss = s.split(":");
			for(String sss : ss[1].split(" "))
				itemGroupNum.put(Integer.parseInt(sss), Integer.parseInt(ss[0]));
		}*/
	}
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			
		String[] line = value.toString().split(" ");
		ArrayList<Integer> list = new ArrayList<Integer>(line.length);
		int item;
		for(int i=1; i<line.length; i++){
			item = Integer.parseInt(line[i]);
			if(allItemsMap.containsKey(item))
				list.add(allItemsMap.get(item));
		}
		Collections.sort(list);
		int[] arrayLine = toIntArray(list);
		HashSet<Integer> groups = new HashSet<Integer>(groupsNum);
		
		for(int i=list.size()-1; i>=0 && groups.size()<groupsNum ; i--) {
			int groupID = itemGroupNum.get(list.get(i));
			
			if(!groups.contains(groupID)) {
				int[] array = Arrays.copyOf(arrayLine, i+1);
				context.write(new IntWritable(groupID), new ValueWritable(array));
			}
			groups.add(groupID);
		}
	}
	
	public void getItem(Context context) throws IOException {
		URI[] paths = context.getCacheFiles();
		if(paths == null || paths.length <=0){
			System.out.println("No DistributedCache keywords File!");
			System.exit(1);
		}
		SequenceFile.Reader reader = null;
		try {
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			for(URI path : paths){	
				if(path.getPath().contains("Flist")) {
					reader = new SequenceFile.Reader(context.getConfiguration(), Reader.file(new Path(path)));
					while (reader.next(key, value)) {
						allItemsMap.put(key.get(), value.get());
					}
				}else if(path.getPath().contains("groupNum")) {
					reader = new SequenceFile.Reader(context.getConfiguration(), Reader.file(new Path(path)));
					while (reader.next(key, value)) {
						itemGroupNum.put(key.get(), value.get());
					}
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			IOUtils.closeStream(reader);
		}
	}
	
	public void getItem_bak(Context context) throws IOException {
		String flist = context.getConfiguration().get("Flist");
		int i = 0;
		for(String str : flist.split(" ")) {
			allItemsMap.put(Integer.parseInt(str), i++);
		}
		String[] str = context.getConfiguration().get("ItemGroup").split(";");
		for(String s : str) {
			String[] ss = s.split(":");
			for(String sss : ss[1].split(" "))
				itemGroupNum.put(Integer.parseInt(sss), Integer.parseInt(ss[0]));
		}
	}
	
	private int[] toIntArray(List<Integer> list) {
	    int[] intArray = new int[list.size()];
	    int ix = 0;
	    for (Integer i : list) {
	      intArray[ix++] = i;
	    }
	    return intArray;
	  }
	
	protected void cleanup(Context context)  throws IOException, InterruptedException{
		super.cleanup(context); 
		allItemsMap = null;
		itemGroupNum = null;
	}
}
