import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class PercenTileDriver {

	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
	       private final static IntWritable one = new IntWritable(1);
	       private Text word = new Text();
	           
	       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	           String line = value.toString();
	           StringTokenizer tokenizer = new StringTokenizer(line);
	           while (tokenizer.hasMoreTokens()) {
	               int number =  Integer.parseInt(tokenizer.nextToken().toString());
	               context.write(NullWritable.get() ,new IntWritable(number) );
	           }
	       }
	    }
	
	public static class Reduce extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
 	   
	       public void reduce(NullWritable key, Iterable<IntWritable> values, Context context) 
	         throws IOException, InterruptedException {
	    	   //System.out.println("In Reducer ::"+ key.toString());
	           int sum = 0;
	           int count =0;
	           double dVal =0;
	           List<Integer> doubleValues = new LinkedList<Integer>();
	           for (IntWritable val : values) {
	               sum += val.get();
	               count++;
	               doubleValues.add(val.get());
	           }
	           
	        int percentile = 50;
	       	float percentage = (sum*percentile)/100;
	       	System.out.println("Percentage = "+percentage);
	       	
	       	/**
	       	 * A class of 20 students had the following scores on their most recent test: 75, 77, 78, 78, 80, 81, 81, 82, 83, 84, 84, 84, 85, 87, 87, 88, 88, 88, 89, 90. 
	       	 * The score of 80% has four scores below it. Since 4/20 = 20%, 80 is the 20th percentile of the class. The score of 90 has 19 scores below it. 
	       	 * Since 19/20 = 95%, 90 corresponds to the 95 percentile of the class
	       	 */
	       	int finalVal =0;
	       	int sum2 = 0 ;
	       	int pos=-1;
	       	for(int i=0;i<doubleValues.size();i++){
	       		sum2+= doubleValues.get(i);
	       		if(sum2>=percentage){
	       			System.out.println(doubleValues.get(i)+" sum2 = "+sum2);
	       			pos= i;
	       			finalVal = doubleValues.get(i);
	       			break;
	       		}
	       	}
	           
	           
	           context.write(NullWritable.get(), new IntWritable(finalVal));
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
        
        job.setJarByClass(PercenTileDriver.class);
        
        job.setOutputKeyClass(NullWritable.class);
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
