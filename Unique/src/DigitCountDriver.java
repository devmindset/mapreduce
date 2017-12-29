import java.io.IOException;
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

/**
 * 
 * @author ubuntu
 * Description : Assume Each line contains a number Find the occurrences of each digit.
 *
 *
 */

public class DigitCountDriver {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
				System.out.println("In Mapper ::" + word.toString());
			}
		}
	}

	public static class Combiner extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
					count++;
			}
		   context.write(key, new IntWritable(count));
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, NullWritable, Text> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += Integer.parseInt(val.toString());
			}
			context.write(NullWritable.get(), new Text(key+" :" +new IntWritable(count)));
		}
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();

		Job job = new Job(conf, "Distinct");

		job.setJarByClass(DigitCountDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/wordcount/numbers.txt"));
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:54310/user/wordcount/numbers.txt"),new Path("hdfs://localhost:54310/user/wordcount/numbers2.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://localhost:54310/user/wordcount/output/result/"));

		// job.waitForCompletion(true);
		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		long endTime = System.currentTimeMillis();
		long executionTime = endTime - startTime;
		System.out.println("executionTime :: " + executionTime);

	}

}
