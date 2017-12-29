package com.practice.hadoop.mapreduce;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		/*if(args.length != 2){
			 System.err.println("Usage: WordCount <input path> <output path>");
		     System.exit(-1);
		}*/
		
		Job job = new Job();
		job.setJarByClass(com.practice.hadoop.mapreduce.WordCountMain.class);
		
		FileInputFormat.addInputPath(job, new Path("input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		job.setMapperClass(com.practice.hadoop.mapreduce.WordCountMapper.class);
		job.setReducerClass(com.practice.hadoop.mapreduce.WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);	
	}

}
