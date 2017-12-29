import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * @author ubuntu Description : In One TB file Numbers are present (One number
 *         each line). How can you find average of the numbers.
 * 
 * 
 */

public class OneTBAverager {

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional
												// '-' and decimal.
	}
	int sumOf =0;
	int countOf =0;
	
	
	public static class SOAverageMapper extends
			Mapper<LongWritable, Text, NullWritable, CountAverageTuple> {

		CountAverageTuple outCountAverage = new CountAverageTuple();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String arr[] = value.toString().split(
					System.getProperty("line.separator"));
			if (null != arr[0] && arr[0].trim().length() > 0
					&& isNumeric(arr[0])) {
				
				if(outCountAverage.getAverage()!=0){
				   outCountAverage.setAverage(outCountAverage.getAverage()+Float.parseFloat(arr[0]));
				} else {
				   outCountAverage.setAverage(Float.parseFloat(arr[0]));	
				}
				
				if(outCountAverage.getCount()!=0){
				   outCountAverage.setCount(outCountAverage.getCount()+1);
				} else {
				   outCountAverage.setCount(1);	
				}
			}
		}
		
		protected void cleanup(
				Mapper<LongWritable, Text, NullWritable, CountAverageTuple>.Context context)
				throws IOException, InterruptedException {
			context.write(NullWritable.get(), outCountAverage);
		}
	}



	public static class AverageReducer
			extends
			Reducer<NullWritable, CountAverageTuple, NullWritable, CountAverageTuple> {
		private CountAverageTuple result = new CountAverageTuple();
	//	private FloatWritable floatData = new FloatWritable(1);

		@Override
		public void reduce(NullWritable key,
				Iterable<CountAverageTuple> values, Context context)
				throws IOException, InterruptedException {

			float sum = 0;
			float count = 0;

			// Iterate through all input values for this key
			for (CountAverageTuple val : values) {
				System.out
						.println(val.getCount() + "------" + val.getAverage());
				sum += val.getAverage();
				count += val.getCount();
			}

			result.setCount(count);
			result.setAverage(sum / count);
			System.out.println(sum + "%%%%%%" + count);
			context.write(NullWritable.get(), result);
		}
	}

	public static class CountAverageTuple implements Writable {
		private float count = 0f;
		private float average = 0f;

		public float getCount() {
			return count;
		}

		public void setCount(float count) {
			this.count = count;
		}

		public float getAverage() {
			return average;
		}

		public void setAverage(float average) {
			this.average = average;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readFloat();
			average = in.readFloat();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(count);
			out.writeFloat(average);
		}

		@Override
		public String toString() {
			return count + "\t" + average;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		/*
		 * String[] otherArgs = new GenericOptionsParser(conf, args)
		 * .getRemainingArgs(); if (otherArgs.length != 2) {
		 * System.err.println("Usage: AverageDriver <in> <out>");
		 * System.exit(2); }
		 */
		Job job = new Job(conf, "Average of 1 TB data");
		job.setJarByClass(OneTBAverager.class);
		job.setMapperClass(SOAverageMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(CountAverageTuple.class);
		FileInputFormat.setInputPaths(job, new Path(
				"hdfs://localhost:54310/user/wordcount/numbers.txt"), new Path(
				"hdfs://localhost:54310/user/wordcount/numbers2.txt"));

		// FileOutputFormat.setOutputPath(job, new
		// Path("hdfs://localhost:54310/user/wordcount/output/result_avg/"));
		Path outputPath = new Path(
				"hdfs://localhost:54310/user/wordcount/output/result_avg/");
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
