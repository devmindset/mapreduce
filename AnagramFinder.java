import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * File containing words.Find Anagram words using Mapreduce.
 */
public class AnagramFinder extends Configured implements Tool{

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key,Text value, Mapper<LongWritable,Text,Text,IntWritable>.Context context ){
			String line = value.toString().replaceAll("\\s+", " ").toLowerCase();
			StringTokenizer st = new StringTokenizer(line," ");
			while (st.hasMoreTokens()){
				String str= st.nextToken();
				char[] arr = str.toCharArray();
				Arrays.sort(arr);
				try {
					String result = "";
					for(char s:arr){
						result+=s;
					}
					context.write(new Text(result),new IntWritable(1));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text,IntWritable,Text,IntWritable>.Context context){
			int sum =0;
			
			for(IntWritable val : values){
				sum+=val.get();
			}
			
			if(sum>0){
				try {
					System.out.println("Key="+key.toString()+" - Sum="+sum);
					context.write(key, new IntWritable(sum));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		
	}
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(ToolRunner.run(new Configuration(), new AnagramFinder(), args)==0){
			System.exit(-1);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf, "AnagramFinder");
		job.setJarByClass(AnagramFinder.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		//job.setInputFormatClass(TextInputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/usr/local/hadoop/input/anagram"));
    	Path outputPath= new Path("hdfs://localhost:54310/user/anagram/output/");
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
        System.out.println(job.waitForCompletion(true) ? 0 : 1);
		
		return 0;
	}

}
//ubuntu@ubuntu:/usr/local/hadoop$ hadoop dfs -mkdir -p /usr/local/hadoop/input/anagram
// ubuntu@ubuntu:/usr/local/hadoop$ hadoop dfs -copyFromLocal /home/ubuntu/Documents/mapreducedata/anagram /usr/local/hadoop/input/anagram/
// ubuntu@ubuntu:/usr/local/hadoop$ hadoop dfs -ls /usr/local/hadoop/input/anagram


