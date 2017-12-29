package com.practice.deb.maxminavggivenbyuser;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * @author ubuntu
 * 
 * Problem Statement ::: List of all the User with the max,min,average ratings they have given against any movie
 *
 *bin/hadoop fs -copyFromLocal /home/ubuntu/practicedata/MoviesData/ratings.txt /user/lenscart/ratings.txt
 *bin/hadoop fs -copyFromLocal /home/ubuntu/practicedata/MoviesData/movies.txt /user/lenscart/movies.txt
 *bin/hadoop fs -copyFromLocal /home/ubuntu/practicedata/MoviesData/tags.txt /user/lenscart/tags.txt
 *
 *
 */

public class MaxMinAvgByUserDriver {

	private static HashMap<String, String> movietMap = new HashMap<String, String>();
	enum MYCOUNTER {
	    RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	    }
	 //MovieID::Title::Genres 
	
	
	
	
	//UserID::MovieID::Rating::Timestamp
	public static class MinMaxAvgMapper extends Mapper<LongWritable,Text,Text, FloatWritable>{
		private FloatWritable val = new FloatWritable();
		private Text number = new Text("Key");
		
		public void map(LongWritable key,Text value, Context context) throws InterruptedException,IOException {
			String arr[]= value.toString().split("::");
			val.set(Float.parseFloat(arr[2]));
            context.write(new Text(arr[0]), val);
		}
	}
	
	public static class UsergMapper extends Mapper<LongWritable,Text,Text, FloatWritable>{
		private FloatWritable val = new FloatWritable();
		private Text number = new Text("Key");
		
		public void map(LongWritable key,Text value, Context context) throws InterruptedException,IOException {
			String arr[]= value.toString().split("::");
			val.set(Float.parseFloat(arr[2]));
            context.write(new Text(arr[0]), val);
		}
	}
	
	public static class MinMaxAvgReducer extends Reducer<Text,FloatWritable ,Text, Text>{
		
		
		public void reduce(Text key,Iterable<FloatWritable> values, Context context) throws InterruptedException,IOException {
          float sum =0;
          int count =0;
          float avg = 0;
          float minNum = 0.0f;
          float maxNum = 0.0f;
          String movieName = null;
		  for(FloatWritable val : values){
			 
			  if(val.get()>maxNum){
				  maxNum = val.get();
			  }
			  if(count==0){
				  minNum = val.get();
			  }else if(val.get()<minNum){
				  minNum = val.get();
			  }
			  count++;
			  sum+=val.get();
		  }
		  avg= sum/count; 
		  
		  try {
		       movieName = movietMap.get(key.toString());
		    } catch(Exception e){
		    	movieName = null;
		    }finally {
		    	movieName = ((movieName.equals(null) || movieName.equals("")) ? "NOT-FOUND" : movieName);
		    }
		  
		  StringBuilder sb = new StringBuilder();
		  sb.append("MovieId_MovieName = ");
		  sb.append(key.toString()+"_"+movieName);
		  sb.append(" MaxRating = ");
		  sb.append(maxNum);
		  sb.append(" MinRating = ");
		  sb.append(minNum);
		  sb.append(" AvgRating = ");
		  sb.append(avg);
		  context.write(key, new Text(sb.toString()));
		}
	}
	
	public static class FirstPartitioner extends Partitioner<Text, FloatWritable> {
		@Override
		public int getPartition(Text key, FloatWritable arg1, int numPartitions) {
			// multiply by 127 to perform some mixing
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}
	
	static class GroupComaparator extends WritableComparator {
		public GroupComaparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable tk1, WritableComparable tk2) {
			Text t1 = (Text) tk1;
			Text t2 = (Text) tk2;
			return t1.compareTo(t2);
		}
	}

	class SortKeyComaparator extends WritableComparator {
		public SortKeyComaparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable tk1, WritableComparable tk2) {
			Text t1 = (Text) tk1;
			Text t2 = (Text) tk2;
			return t1.compareTo(t2);
		}
	}


	
	//2547
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException{
		long startTime = System.currentTimeMillis();
	  Configuration conf = new Configuration();
	  Job job = new Job(conf, "CalculateAverage");
	  DistributedCache.addCacheFile(new URI("/user/lenscart/movies.txt"),job.getConfiguration());
	  job.setJarByClass(MaxMinAvgByUserDriver.class);
	  job.setMapperClass(MinMaxAvgMapper.class);
	  job.setReducerClass(MinMaxAvgReducer.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(FloatWritable.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  job.setNumReduceTasks(4);
	 // job.setPartitionerClass(FirstPartitioner.class); 
	 // job.setSortComparatorClass(SortKeyComaparator.class); 
	  //job.setGroupingComparatorClass(GroupComaparator.class); 
	  
	  
	  FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/lenscart/ratings.txt"));
	  Path outputPath = new Path("hdfs://localhost:54310/user/lenscart/output/");
      FileOutputFormat.setOutputPath(job, outputPath);
      outputPath.getFileSystem(conf).delete(outputPath);
	  //System.exit(job.waitForCompletion(true) ? 0 : 1);
      int returnValue = job.waitForCompletion(true) ? 0 : 1;
	  long endTime = System.currentTimeMillis();
		long executionTime = endTime- startTime;
		System.out.println("executionTime :: "+executionTime);
		//60452 - part,group
		//57182 - part
		//49387/45674- nothing
	}
	
	
}
