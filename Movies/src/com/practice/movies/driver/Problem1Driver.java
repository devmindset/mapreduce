package com.practice.movies.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Problem1Driver {

	private final static IntWritable one = new IntWritable(1);
	public static class MovieMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	       private final static IntWritable one = new IntWritable(1);
	       private Text word = new Text();
	           
	       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	           String line = value.toString();
	           String[] keyVal = line.split("::");
	           context.write(new Text(keyVal[1]),one );
	       }
	}
	
	
	public static class MovieCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
		//Logger logger = LoggerFactory.getLogger(WordCountCombiner.class);

	    protected void reduce(Text key, Iterable<IntWritable> values,
	                          Context context)
	            throws IOException, InterruptedException {
	        //System.out.println("Entering WordCountCombiner.reduce() " );
	        int sum = 0;
	        for(IntWritable iw : values){
	        	sum += iw.get();
	        }
	        //logger.debug(key + " -> " + sum);
	        context.write(key, new IntWritable(sum));
	        //logger.debug("Exiting WordCountCombiner.reduce()");
	    }
	}
	
	
	public static class MovieReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
 	   
	       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	         throws IOException, InterruptedException {
	           int sum = 0;
	           for (IntWritable val : values) {
	               sum += val.get();
	           }
	           context.write(key, new IntWritable(sum));
	       }
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration(); 
		Job job = new Job(conf, "CalculateRatingPerMovie");
		job.setJarByClass(Problem1Driver.class);
		job.setJobName("WordCounter");
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/movie/ratings.txt"));
        //FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/wordcount/output/result/"));
		Path outputPath= new Path("hdfs://localhost:54310/user/movie/output/");
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MovieMap.class);
		job.setReducerClass(MovieReducer.class);
		job.setCombinerClass(MovieCombiner.class);
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		long endTime = System.currentTimeMillis();
		long executionTime = endTime- startTime;
		System.out.println("executionTime :: "+executionTime);
		
		
	}

}
