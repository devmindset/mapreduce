import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CounterDriver {
	private enum COUNTERS {
		GOOD, BAD
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);

		job.setMapperClass(MapTask.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/wordcount/Counters.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/wordcount/output/result/"));

		job.waitForCompletion(false);

		Counters counters = job.getCounters();

		System.out.printf("Good: %d, Bad: %d\n",
				counters.findCounter(COUNTERS.GOOD).getValue(), counters
						.findCounter(COUNTERS.BAD).getValue());
	}

	public static class MapTask extends
			Mapper<LongWritable, Text, Text, NullWritable> {
		private Pattern p = Pattern
				.compile("^([\"']?)\\d\\d:\\d\\d\\1,([\"']?)[A-Z]\\w+\\2,.*$");

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (p.matcher(value.toString()).matches()) {
				context.getCounter(COUNTERS.GOOD).increment(1L);
				context.write(value, NullWritable.get());
			} else {
				context.getCounter(COUNTERS.BAD).increment(1L);
			}
		}
	}
}
