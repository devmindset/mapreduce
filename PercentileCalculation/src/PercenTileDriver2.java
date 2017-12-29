import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author ubuntu
 *
 *Your percentile score = { (No, of people who got less than you/ equal to you) / (no. of people appearing in the exam) } x 100 
 *Thus, if in an exam, 10 people give the test and 9 people get either less than what you got or equal to what you got, your percentile score is: 
 *{9/10} x 100 = 90 percentile. 
 *
 */

public class PercenTileDriver2 {

	
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	       private final static IntWritable one = new IntWritable(1);
	       private Text word = new Text();
	           
	       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	           String line = value.toString();
	           StringTokenizer tokenizer = new StringTokenizer(line);
	           while (tokenizer.hasMoreTokens()) {
	               int number =  Integer.parseInt(tokenizer.nextToken().toString().trim());
	               context.write(one ,new IntWritable(number) );
	           }
	       }
	    }
	
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable > {
 	   
	       public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
	         throws IOException, InterruptedException {
	           int val = 0;
	           int count =0;
	           int lastIndex = 0;
	           double percentile = 0;
	           System.out.println("Hello");
	           List<Integer> doubleValues = new ArrayList<Integer>();
	           for (IntWritable vals : values) {
	               count++;
	               doubleValues.add(vals.get());
	           }
	           System.out.println("Hello2");
	           for(int i=0;i<count;i++){
	        	   val = doubleValues.get(i);
	        	   System.out.println(val+"*********"+doubleValues);
	        	   lastIndex = doubleValues.indexOf(new Integer(val));   
	        	   System.out.println(val+"*********"+lastIndex);
	        	   percentile = (((lastIndex+1)*100)/count);
	        	   context.write(new IntWritable(val), new DoubleWritable(percentile));
               }
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
            
        Job job = new Job(conf, "wordcount");
        
        job.setJarByClass(PercenTileDriver2.class);
        //This will be Map Output key,value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/percentile/percentile.txt"));
        Path outputPath= new Path("hdfs://localhost:54310/user/percentile/output/");
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
            
        //job.waitForCompletion(true);
        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        long endTime = System.currentTimeMillis();
		long executionTime = endTime- startTime;
		System.out.println("executionTime :: "+executionTime);

	}

}
