import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TotalOrderParttionerDriver {

	/**
	 * Mapper class for the dictionary sort task. This is an identity class
	 * which simply outputs the key and the values that it gets, the
	 * intermediate key is the document frequency of a word.
	 * 
	 * @author UP
	 * 
	 */
	public static class SortMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String val = value.toString();
			if (val != null && !val.isEmpty() && val.length() >= 5) {
				String[] splits = val.split(",");
				context.write(new LongWritable(Long.parseLong(splits[1])),
						new Text(splits[0] + "," + splits[2]));
			}
		}
	}
	

	 
	/**
	 * Reducer class for the dictionary sort class. This is an identity reducer
	 * which simply outputs the values received from the map output.
	 *
	 * @author UP
	 *
	 */
	public static class SortReducer extends
	        Reducer<LongWritable, Text, Text, LongWritable> {
	 
	    protected void reduce(LongWritable key, Iterable<Text> value, Context context)
	            throws IOException, InterruptedException {
	        for(Text val : value) {
	            context.write(new Text(val + "," + key), null);
	        }
	    }
	}	
	
	/**
	 * This comparator is used to sort the output of the dictionary in the
	 * descending order of the counts. The descending order enables to pick a
	 * dictionary of the required size by any aplication using the dictionary.
	 *
	 * @author UP
	 *
	 */
	public static class SortKeyComparator extends WritableComparator {
	     
	    protected SortKeyComparator() {
	        super(LongWritable.class, true);
	    }
	 
	    /**
	     * Compares in the descending order of the keys.
	     * -1 for descending order
	     */
	    public int compare(WritableComparable a, WritableComparable b) {
	        LongWritable o1 = (LongWritable) a;
	        LongWritable o2 = (LongWritable) b;
	        return -1 * o1.compareTo(o2);
	    }
	     
	}	
	
	public static void main(String[] args){

		int numReduceTasks = 2;
		
	       Configuration conf = new Configuration();
	        Job job = new Job(conf, "DictionarySorter");
	        job.setJarByClass(TotalOrderParttionerDriver.class);
	        job.setMapperClass(SortMapper.class);
	        job.setReducerClass(SortReducer.class);
	        job.setPartitionerClass(TotalOrderPartitioner.class);
	        job.setNumReduceTasks(numReduceTasks);
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setOutputFormatClass(TextOutputFormat.class);
	 
	        job.setOutputKeyClass(LongWritable.class);
	        job.setOutputValueClass(Text.class);
	        job.setSortComparatorClass(SortKeyComparator.class);
	 
	        FileInputFormat.setInputPaths(job, input);
	        FileOutputFormat.setOutputPath(job, new Path(output
	                + ".dictionary.sorted." + getCurrentDateTime()));
	        //job.setPartitionerClass(TotalOrderPartitioner.class);
	 
	        Path inputDir = new Path(partitionLocation);
	        Path partitionFile = new Path(inputDir, "partitioning");
	        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),
	                partitionFile);
	 
	        double pcnt = 10.0;
	        int numSamples = numReduceTasks;
	        int maxSplits = numReduceTasks - 1;
	        if (0 >= maxSplits)
	            maxSplits = Integer.MAX_VALUE;
	 
	        InputSampler.Sampler sampler = new InputSampler.RandomSampler(pcnt,
	                numSamples, maxSplits);
	        InputSampler.writePartitionFile(job, sampler);
	 
	        try {
	            job.waitForCompletion(true);
	        } catch (InterruptedException ex) {
	           // logger.error(ex);
	        } catch (ClassNotFoundException ex) {
	            //logger.error(ex);
	        }
		
		
		/*JobConf conf=new JobConf(null,TotalOrderParttionerDriver.class);
		conf.setJobName("TotalOrderParttionerDriver");
		
		conf.setPartitionerClass(TotalOrderPartitioner.class);
 
		conf.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) SortMapper.class);
		conf.setReducerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) SortReducer.class);
		conf.setPartitionerClass(TotalOrderPartitioner.class);
		conf.setNumReduceTasks(numReduceTasks);
		conf.setInputFormat((Class<? extends InputFormat>) TextInputFormat.class);
		conf.setOutputFormat((Class<? extends OutputFormat>) TextOutputFormat.class);
	 
	        conf.setOutputKeyClass(LongWritable.class);
	        conf.setOutputValueClass(Text.class);
	        job.setSortComparatorClass(SortKeyComparator.class);
	 
	        String input = "";
	        String output = "";
	        FileInputFormat.setInputPaths(job, input);
	        FileOutputFormat.setOutputPath(job, new Path(output));
	        job.setPartitionerClass(TotalOrderPartitioner.class);
	 
	        Path inputDir = new Path(partitionLocation);
	        Path partitionFile = new Path(inputDir, "partitioning");
	        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partitionFile);*/
		
		
		
	}
	

}
