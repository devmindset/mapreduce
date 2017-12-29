import java.io.IOException;
import java.util.StringTokenizer;

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


public class WordCountCombinerDriver {
	
	
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	       private final static IntWritable one = new IntWritable(1);
	       private Text word = new Text();
	           
	       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	           String line = value.toString();
	           StringTokenizer tokenizer = new StringTokenizer(line);
	           while (tokenizer.hasMoreTokens()) {
	               word.set(tokenizer.nextToken());
	               context.write(word, one);
	           }
	       }
	    }

	public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
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
	
	   public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    	   
	       public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	         throws IOException, InterruptedException {
	           int sum = 0;
	           for (IntWritable val : values) {
	               sum += val.get();
	           }
	           context.write(key, new IntWritable(sum));
	       }
	    }
	
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration(); 
		Job job = new Job(conf, "CalculateAverage");
		job.setJarByClass(WordCountCombinerDriver.class);
		job.setJobName("WordCounter");
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/wordcount/wordcount.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/wordcount/output/result/"));
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountCombiner.class);
	
		int returnValue = job.waitForCompletion(true) ? 0:1;
		System.out.println("job.isSuccessful " + job.isSuccessful());
		long endTime = System.currentTimeMillis();
		long executionTime = endTime- startTime;
		System.out.println("executionTime :: "+executionTime);
	}
	
}
