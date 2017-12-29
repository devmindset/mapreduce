package com.practice.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text arg0, java.lang.Iterable<IntWritable> arg1, org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context arg2) throws java.io.IOException ,InterruptedException {
		int sum = 0;
		while(arg1.iterator().hasNext()){
			IntWritable intWritable = arg1.iterator().next();
			sum += intWritable.get();
		}
		arg2.write(arg0, new IntWritable(sum));
	}
}
