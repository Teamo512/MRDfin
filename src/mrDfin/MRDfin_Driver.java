package mrDfin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRDfin_Driver extends Configured implements Tool{
	
	private String[] moreParas;  //additional parameters that are pairs
	private static String dataBaseName;
	private double relativeSup; //relative support
	public static int minSup; // minimum support
	private String inDir;
	private String outDir;
	private int dataSize;  // the number of transactions in dataBase
	private static int childJavaOpts;  //set jvm memoery
	private static int mapperNum;
	private static int reducerNum;
	private static int groupNum;  //the number of group
	
	private String input;
	private String output;
	
	public boolean useFileCache; 
	
	
	public static final String PART = "part";
	public static final String FrequentItem = "frequentItem";
	
	private ArrayList<Long> eachLevelItemsetNum = new ArrayList<Long>();  //save the number of frequent pattern at each level
	private ArrayList<Double> eachLevelRunningTime = new ArrayList<Double>();  //save time at each level
	
	
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		conf.setLong("minSup", minSup);
		conf.setInt("dataSize", dataSize);
		conf.set("mapreduce.map.java.opts", "-Xmx"+childJavaOpts+"M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx"+2*childJavaOpts+"M");
		conf.set("mapreduce.task.timeout", "6000000");
		conf.setInt("mapperNum", mapperNum);
	
		useFileCache = false;
		conf.setBoolean("Cache", useFileCache);
		
		//命令行的额外参数，必须是成对出现的，分别为配置文件的的属性名和属性值。
		for( int k = 0; k < moreParas.length && moreParas.length >= 2; k+=2) {
			conf.set( moreParas[ k ], moreParas[ k+1 ] );			
		}
		
		try {
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(new Path(outDir)))
				fs.delete(new Path(outDir), true);
			
			long startTime = System.currentTimeMillis();
			input = inDir;
			output = outDir + "/1";
			runFirst(conf);
		
			if(useFileCache)
				partition(output, conf);
			else
				partition_bak(output, conf);
			
			output = outDir + "/2";
			runSecond(conf);
		
			fs.close();
			long endTime=System.currentTimeMillis();
			System.out.println(endTime - startTime);
			saveResult((endTime - startTime));
		}catch(Exception e) {
			e.printStackTrace();
			File file = new File("MRDfin_" + dataBaseName + "_ResultOut");
			BufferedWriter br  = new BufferedWriter(new FileWriter(file, true));  // true means appending content to the file //here create a non-existed file
			br.write("MRDfin Exception occurs at minimumSupport(relative) "  + relativeSup);
			br.write("\n");
			br.flush();
			br.close();
		}
		return 0;
	}
	
	//First Job:counting F1 and support
	public void runFirst(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		Path inPath = new Path(input);
		Path outputPath = new Path(output);
		
		Job job = Job.getInstance(conf, "First_MRDfin");
		job.setJarByClass(MRDfin_Driver.class);
		
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(FirstMapper.class);
		//job.setCombinerClass(FirstCombiner.class);
		job.setReducerClass(FirstReducer.class);
		//job.setMapOutputKeyClass(IntWritable.class);
		//job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(reducerNum); 
		
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);
		long endTime = System.currentTimeMillis();
		
		saveEveryJobResult((endTime - startTime), job);
		System.out.println(endTime - startTime);
		
	}
	
	//Second Job: partition the prime database
	public void runSecond(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException{
		
		Path inPath = new Path(input);
		Path outputPath = new Path(output);
		
		Job job = Job.getInstance(conf, "Second_MRDfin");
		job.setJarByClass(MRDfin_Driver.class);
		
		if(useFileCache) {
			job.addCacheFile(URI.create(outDir + "/" + "Flist"));
			job.addCacheFile(URI.create(outDir + "/" + "groupNum"));
		}
		
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(SecondMapper.class);
		job.setReducerClass(SecondReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ValueWritable.class);
	
		job.setNumReduceTasks(reducerNum);
		
		long startTime = System.currentTimeMillis();
		job.waitForCompletion(true);		
		long endTime = System.currentTimeMillis();
		
		saveEveryJobResult((endTime - startTime), job);
		System.out.println(endTime - startTime);
	}
	
	public static void saveAllToCache(String dir, Job job) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(dir),job.getConfiguration());
		FileStatus[] stats = fs.listStatus(new Path(dir));
		for(FileStatus file : stats){
			if(file.getPath().getName().contains("part"))
				job.addCacheFile(URI.create(file.getPath().toString()));
		}
	}
	
	public void saveEveryJobResult(long time, Job job){
		Counters counter = null;
		try{
			counter = job.getCounters();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		eachLevelItemsetNum.add(counter.findCounter(MRDfinCounter.TatolFrequentNum).getValue());
		eachLevelRunningTime.add(time/1000.0);
	}
	
	//对一阶模式进行分组
	public void partition(String uri, Configuration conf) {
		HashMap<Integer,Integer> allItemsMap = new HashMap<Integer,Integer>();
		int[] itemSup;
		SequenceFile.Reader reader = null;
		SequenceFile.Writer writer = null;
		FileSystem fs = null;
		try{
			fs =  FileSystem.get(conf);
			FileStatus[] fileStatus = fs.listStatus(new Path(uri));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			for(Path path : paths){
				if(path.toString().contains(PART)) {
					reader = new SequenceFile.Reader(conf, Reader.file(path));
				
					while (reader.next(key, value)){
						allItemsMap.put(key.get(), value.get());
					}
					
				} 
			}
			reader.close();
			if(allItemsMap != null) {	
				//items are reversed and sorted by support descending order
				List<Entry<Integer,Integer>> list = new ArrayList<Entry<Integer,Integer>>(allItemsMap.entrySet());
				
				list.sort(new Comparator<Entry<Integer, Integer>>() {
					@Override
					public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
						return Objects.equals(o1.getValue(), o2.getValue()) ? (o1.getKey() - o2.getKey()) : (o2.getValue() - o1.getValue());
					}
				});
				allItemsMap = null;
				writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(new Path(outDir + "/" + "Flist")), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(IntWritable.class));
				itemSup = new int[list.size()];
				int i=0;
				for(Entry<Integer,Integer> entry : list){
					writer.append(new IntWritable(entry.getKey()), new IntWritable(i));//根据支持度降序，给模式重新赋值，从0开始递增
					itemSup[i++] = entry.getValue();//记录赋值后的模式与其支持度的对应关系
				}
				writer.close();
				list = null;
				
				saveItemGroup(outDir + "/" + "groupNum", itemSup, conf, groupNum);
			}
			
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(reader);
			IOUtils.closeStream(writer);
		}
		
	}
	
	public void partition_bak(String uri, Configuration conf) {
		HashMap<Integer,Integer> allItemsMap = new HashMap<Integer,Integer>();
		//Item[] itemSup;
		SequenceFile.Reader reader = null;
		FileSystem fs = null;
		try{
			fs =  FileSystem.get(conf);
			FileStatus[] fileStatus = fs.listStatus(new Path(uri));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			for(Path path : paths){
				if(path.toString().contains(PART)) {
					reader = new SequenceFile.Reader(conf, Reader.file(path));
				
					while (reader.next(key, value)){
						allItemsMap.put(key.get(), value.get());
					}
					
				} 
			}
			reader.close();
			if(allItemsMap != null) {	
				//items are reversed and sorted by support descending order
				List<Entry<Integer,Integer>> list = new ArrayList<Entry<Integer,Integer>>(allItemsMap.entrySet());
				
				list.sort(new Comparator<Entry<Integer, Integer>>() {
					@Override
					public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
						return Objects.equals(o1.getValue(), o2.getValue()) ? (o1.getKey() - o2.getKey()) : (o2.getValue() - o1.getValue());
					}
				});
				allItemsMap = null;
				//itemSup = new Item[list.size()];
				int[] itemSup = new int[list.size()];
				StringBuilder flist = new StringBuilder();
				int i=0;
				for(Entry<Integer,Integer> entry : list){
					flist.append(entry.getKey()+" ");
					itemSup[i++] = entry.getValue();//记录赋值后的模式与其支持度的对应关系
				}
				conf.set("Flist", flist.toString().trim());
				list = null;
				
				saveItemGroup_bak(itemSup, conf, groupNum);
			}
			
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(reader);
		}
		
	}
	
	//根据分组个数，将一阶模式分组，并采用文件的形式保存
	public void saveItemGroup(String uri, int[] itemSup, Configuration conf, int N) {
		SequenceFile.Writer writer = null;
		int M = itemSup.length;
		try {
			Path path = new Path(uri);
			writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(IntWritable.class));
			if(M <= N) {  //模式个数M不足分组数大小N时，就分成M组，否则就分成N组
				conf.setInt("GroupsNum", M);
				for(int i = 0; i < M-1; i++) {
					writer.append(new IntWritable(i), new IntWritable(i));
				}
				
			}else {  // 按贪心算法策略进行分组，每次都将下一个模式存入最小的分组中
				conf.setInt("GroupsNum", N);
				int j = 0;
				int[] group = new int[N];
				for(; j<N; j++) {
					writer.append(new IntWritable(j), new IntWritable(j));
					group[j] = itemSup[j];
				}
				for(;j<M; j++) {
					int smallIndex = getIndex(group);
					writer.append(new IntWritable(j), new IntWritable(smallIndex));
					group[smallIndex] += itemSup[j];
				}
			}
		
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if (writer != null) {
				IOUtils.closeStream(writer);
			}
		}
	}
	
	//分组，不写入文件，而是采用conf。set形式，写入配置文件，进行传递。这是考虑到一阶模式个数不大，在10000个以内都可以。
	public void saveItemGroup_bak(int[] itemSup, Configuration conf, int N) {
		StringBuilder str = new StringBuilder();
		int M = itemSup.length;
		try {
			if(M <= N) {  //模式个数M不足分组数大小N时，就分成M组，否则就分成N组
				conf.setInt("GroupsNum", M);
				for(int i = 0; i < M-1; i++) {
					str.append(i+":"+i);
					str.append(";");
				}
				str.append(M-1+":"+ (M-1));
				
			}else {  // 按贪心算法策略进行分组，每次都将下一个模式存入最小的分组中
				conf.setInt("GroupsNum", N);
				StringBuilder[] array = new StringBuilder[N];
				int j = 0;
				int[] group = new int[N];
				for(; j<N; j++) {
					group[j] = itemSup[j];
					array[j] = new StringBuilder();
					array[j].append(j + " ");
				}
				for(;j<M; j++) {
					int smallIndex = getIndex(group);
					group[smallIndex] += itemSup[j];
					array[smallIndex].append(j + " ");
				}
				for(int i = 0; i < N-1; i++) {
					str.append(i+":"+array[i].toString().trim()+";");
				}
				str.append(N-1+":"+array[N-1].toString().trim());
				
			}
			conf.set("ItemGroup", str.toString());
		
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	//获取数组中最小元素的数组下标
	public int getIndex(int[] group) {
		int j=0;
		for(int i=0; i<group.length; i++) {
			if(group[i]<group[j])
				j=i;
		}
		return j;
		
	}
	
	//保存输出信息，(包括数据库名，各阶段时间，挖掘模式等信息)
	public void saveResult(long time) {
		try {

			BufferedWriter br = null;
			long TotalItemsetsNum = 0;
			double TotalJobsRunningTime = 0;
			int k = eachLevelItemsetNum.size();
			for(int j=0; j<k; j++) {
				TotalItemsetsNum += eachLevelItemsetNum.get(j);
				TotalJobsRunningTime += eachLevelRunningTime.get(j);
			}

			File resultFile = new File("MRDfin_" + dataBaseName + "_ResultOut");
			
			if( !resultFile.exists() ) {
				br  = new BufferedWriter(new FileWriter(resultFile, true));  // true means appending content to the file //here create a non-existed file
				br.write("algorithmName" + "\t" + "datasetName" + "\t" + "minSuppPercentage(relative)" + "\t" + "minSupp(absolute)" +  "\t" + "TotalTime" + "\t");
				br.write("TotalItemsetsNum" + "\t" + "TotalJobsRunningTime" + "\t");
				
				br.write("Level1TotalItemsetsNum" + "\t" + "Level1JobRunningTime" + "\t");
				
				br.write("Level2TotalItemsetsNum" + "\t" + "Level2JobRunningTime" + "\t");
				
				br.write("Level3TotalItemsetsNum" + "\t" + "Level3JobRunningTime" + "\t");
				
				
				for( int i = 0; i<moreParas.length&&moreParas.length > 1; i=i+2)  {
					br.write(moreParas[i] + "\t");
				}
				
				br.write("\n");
				
			} else {
				br  = new BufferedWriter(new FileWriter(resultFile, true));  // true means appending content to the file
			}
			
			
			br.write("MRDfin" + "\t" + dataBaseName + "\t"  + relativeSup*100.0 + "\t" +  minSup +  "\t" + (time/1000.0) + "\t");
			
			br.write(TotalItemsetsNum + "\t" + TotalJobsRunningTime + "\t");
		
			for(int j=0; j<k; j++) {	
				br.write(eachLevelItemsetNum.get(j) + "\t" +eachLevelRunningTime.get(j) + "\t");
				 	
			}
			
			for( int i = 1; i<moreParas.length&&moreParas.length > 1; i=i+2)  {
				br.write(moreParas[i] + "\t");
			}
			
			br.write("\n");
			br.flush();
			br.close();			
		} catch(IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public MRDfin_Driver(String[] args){
		int numFixedParas = 9;
		int numMoreParas = args.length - numFixedParas;
		if(args.length < numFixedParas || numMoreParas % 2 != 0){
			System.out.println("The Number of the input parameters is Wrong!!");
			System.exit(1);
		}else{
			if(numMoreParas > 0 ){
				moreParas = new String[numMoreParas];
				for(int k = 0; k < numMoreParas; k++) {
					moreParas[k] = args[numFixedParas + k];
				}
			} else {
				moreParas = new String [1] ;
			}
		}
		dataBaseName = args[0];
		relativeSup = Double.parseDouble(args[1]);
		minSup = (int)Math.ceil(relativeSup * Integer.parseInt(args[4]));
		inDir = args[2];
		outDir = args[3];
		dataSize = Integer.parseInt(args[4]);
		childJavaOpts = Integer.valueOf(args[5]);
		mapperNum = Integer.valueOf(args[6]);
		reducerNum = Integer.valueOf(args[7]);
		groupNum = Integer.valueOf(args[8]);
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new MRDfin_Driver(args), args);
		System.out.println(res);
	}

}
