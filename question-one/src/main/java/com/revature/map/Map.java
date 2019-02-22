package com.revature.map;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] arr = line.split("[^a-zA-Z-']+");
		
		for(String word: arr) {
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
