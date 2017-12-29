import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MinMaxDriver {
	
	public static boolean isNumeric(String str)	{
		  return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
		}
		
		public static class MinMaxMapper extends Mapper<LongWritable,Text,Text, FloatWritable>{
			private FloatWritable val = new FloatWritable();
			private Text number = new Text("Key");
			
			public void map(LongWritable key,Text value, Context context) throws InterruptedException,IOException {
				String arr[]= value.toString().split(System.getProperty("line.separator"));
				if(null!=arr[0] && arr[0].trim().length()>0 && isNumeric(arr[0])){
				   val.set(Float.parseFloat(arr[0]));
	               context.write(number, val);
				}
			}
		}
		
		public static class MinMaxReducer extends Reducer<Text,FloatWritable ,NullWritable, FloatWritable>{
			NullWritable nw = NullWritable.get();
			FloatWritable fout =  new FloatWritable();
			public void reduce(Text key,Iterable<FloatWritable> values, Context context) throws InterruptedException,IOException {
              float sum =0;
              int count =0;
              float minNum = 0.0f;
              float maxNum = 0.0f;
			  for(FloatWritable val : values){
				  if(val.get()>maxNum){
					  maxNum = val.get();
				  }
				  if(val.get()<minNum){
					  minNum = val.get();
				  }
			  }
			   
			  context.write(nw, new FloatWritable(maxNum));
			  context.write(nw, new FloatWritable(minNum));
			}
		}	
	
	
	
public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
	Configuration conf = new Configuration();
//  if (args.length != 2) {
//      System.err.println("Usage: Wordcount <in> <out>");
//      System.exit(2);
//  }
  Job job = new Job(conf, "CalculateAverage");
  job.setJarByClass(MinMaxDriver.class);
  job.setMapperClass(MinMaxMapper.class);
  job.setReducerClass(MinMaxReducer.class);
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(FloatWritable.class);
  job.setOutputKeyClass(NullWritable.class);
  job.setOutputValueClass(FloatWritable.class);
//  FileInputFormat.addInputPath(job, new Path(args[0]));
//  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/wordcount/numbers.txt"));
  FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/wordcount/output/result_minmax/"));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
}

}
