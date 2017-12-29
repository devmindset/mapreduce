
/**
 * 
 * NOT WORKING
 * 
 * 
 */



import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
           
   public class Wordcounting {
	   
	   

	   public static class WordCountMapper extends  Mapper<LongWritable, Text, Text, IntWritable>
	   {
	         //hadoop supported data types
	         private final static IntWritable one = new IntWritable(1);
	         private Text word = new Text();
	        
	         //map method that performs the tokenizer job and framing the initial key value pairs
	         public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output) throws IOException
	         {
	        	 System.out.println("Hello");
	               //taking one line at a time and tokenizing the same
	               String line = value.toString();
	             StringTokenizer tokenizer = new StringTokenizer(line);
	            
	             //iterating through all the words available in that line and forming the key value pair
	               while (tokenizer.hasMoreTokens())
	               {
	                  word.set(tokenizer.nextToken());
	                  //sending to output collector which inturn passes the same to reducer
	                    output.collect(word, one);
	               }
	          }
	   }	   
	   
	   
	   public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	   {
	         //reduce method accepts the Key Value pairs from mappers, do the aggregation based on keys and produce the final out put
	         public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output) throws IOException
	         {
	               int sum = 0;
	               /*iterates through all the values available with a key and add them together and give the
	               final result as the key and sum of its values*/
	             while (values.hasNext())
	             {
	                  sum += values.next().get();
	             }
	             output.collect(key, new IntWritable(sum));
	         }
	   }
           
	   
	   public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
		 //creating a JobConf object and assigning a job name for identification purposes
		   Configuration conf = new Configuration();
           
	        Job job = new Job(conf, "wordcount");

	        job.setJarByClass(Wordcounting.class);
	        
           //Setting configuration object with the Data Type of output Key and Value
           job.setOutputKeyClass(Text.class);
           job.setOutputValueClass(IntWritable.class);

           //Providing the mapper and reducer class names
           job.setMapperClass(WordCountMapper.class);
           job.setReducerClass(WordCountReducer.class);

           
  
           //the hdfs input and output directory to be fetched from the command line
//           FileInputFormat.addInputPath(conf, new Path(args[0]));
//           FileOutputFormat.setOutputPath(conf, new Path(args[1]));

           
           FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/wordcount/wordcount.txt"));
           //FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/wordcount/output/result/"));
           Path outputPath= new Path("hdfs://localhost:54310/user/wordcount/output/result/");
   		   FileOutputFormat.setOutputPath(job, outputPath);
   		   outputPath.getFileSystem(conf).delete(outputPath);
   	       int returnValue = job.waitForCompletion(true) ? 0 : 1;

	   }
	   
   /* public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    	 private final static IntWritable cnt = new IntWritable(1);
         private Text text = new Text();

           
       public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException, InterruptedException {
           String line = value.toString();
           StringTokenizer tokenizer = new StringTokenizer(line);
           while (tokenizer.hasMoreTokens()) {
               text.set(tokenizer.nextToken());
               output.collect(text, cnt);
           }
           //System.out.println("Mapper");
       }
    }
    
    
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    	   
    	       public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,Reporter reporter) 
    	         throws IOException, InterruptedException {
    	           int sum = 0;
    	         while (values.hasNext()) {
    	           sum += values.next().get();
    	         }
    	        // System.out.println("Reducer");
    	         output.collect(key, new IntWritable(sum));
    	       }
    	    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
            
        Job job = new Job(conf, "wordcount");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/wordcount/wordcount.txt"));
        //FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/wordcount/output/result/"));
        Path outputPath= new Path("hdfs://localhost:54310/user/wordcount/output/result/");
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
                   
       
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
     }*/
    
    
    
   }
