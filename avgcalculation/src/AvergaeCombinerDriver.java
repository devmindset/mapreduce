import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author ubuntu Description : In One TB file Numbers are present (One number
 *         each line). How can you find average of the numbers.
 * 
 * 
 */

public class AvergaeCombinerDriver {

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional
												// '-' and decimal.
	}

	public static class SOAverageMapper extends
			Mapper<Object, Text, FloatWritable, CountAverageTuple> {

		private FloatWritable floatData = new FloatWritable();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			CountAverageTuple outCountAverage = new CountAverageTuple();

			String arr[] = value.toString().split(
					System.getProperty("line.separator"));
			if (null != arr[0] && arr[0].trim().length() > 0
					&& isNumeric(arr[0])) {
				outCountAverage.setAverage(Float.parseFloat(arr[0]));
				outCountAverage.setCount(1);
				floatData.set(Float.parseFloat(arr[0]));
				context.write(floatData, outCountAverage);
			}

		}
	}

	public static class AverageCombiner
			extends
			Reducer<FloatWritable, CountAverageTuple, FloatWritable, CountAverageTuple> {
		private CountAverageTuple result = new CountAverageTuple();
		private FloatWritable floatData = new FloatWritable(1);

		@Override
		public void reduce(FloatWritable key,
				Iterable<CountAverageTuple> values, Context context)
				throws IOException, InterruptedException {

			float sum = 0;
			float count = 0;

			// Iterate through all input values for this key
			for (CountAverageTuple val : values) {
				System.out.println(val.getCount() + "****" + val.getAverage());
				sum += val.getAverage();
				count += val.getCount();
			}

			result.setCount(count);
			result.setAverage(sum);

			context.write(floatData, result);
		}
	}

	public static class AverageReducer
			extends
			Reducer<FloatWritable, CountAverageTuple, FloatWritable, CountAverageTuple> {
		private CountAverageTuple result = new CountAverageTuple();
		private FloatWritable floatData = new FloatWritable(1);

		@Override
		public void reduce(FloatWritable key,
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
			context.write(floatData, result);
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
		job.setJarByClass(AverageDriver.class);
		job.setMapperClass(SOAverageMapper.class);
		job.setCombinerClass(AverageCombiner.class);
		job.setReducerClass(AverageReducer.class);
		job.setOutputKeyClass(FloatWritable.class);
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
