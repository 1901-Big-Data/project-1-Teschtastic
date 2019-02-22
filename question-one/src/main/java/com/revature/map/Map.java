package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {

		Text word = new Text();
		DoubleWritable avg = new DoubleWritable();
		
		String header = value.toString();
		header = header.replace(",", ", ");
		String[] arr = header.split(",");
		Double sum = 0.0;
		
		for(String col: arr) {
			System.out.println("col="+col);
			System.out.println("arr length="+arr.length);
			if(col == arr[2]) {
				word = new Text(col.trim());
			}
			// 4 - 60 for years 
			for(int i = 4; i < 6; i++) {
				if (col == arr[i]) {
					if (arr[i].equals(" "))
						arr[i] = "0.0".trim();
					sum += Double.parseDouble(arr[i]);
				}
			}
			//sum /= arr.length - 4;
			avg = new DoubleWritable(sum);
		}
		context.write(word, avg);
	}
}
