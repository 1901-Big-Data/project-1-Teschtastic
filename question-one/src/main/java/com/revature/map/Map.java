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
		String country = new String();
		String item = new String();
		header = header.replace(",", ", ");
		String[] arr = header.split(",");
		Double sum = 0.0;
		int count = 0;
		
		for(String col: arr) {
			if(col == arr[1]) {
				country = col;
			}
			if(col == arr[3]) {
				item = col;
			}
			// 4 - 60 for years 
			for(int i = 4; i < 6; i++) {
				if (col == arr[i]) {
					if(arr[i].equals(" "))
						arr[i] = "0.0".trim();
					if(!arr[i].equals("0.0"))
						count++;
					sum += Double.parseDouble(arr[i]);
				}
			}
		}
		
		if(count == 0)
			count = 1;
		
		sum /= count;
		word = new Text(country.trim() + " " + item.trim());
		avg = new DoubleWritable(sum);
		context.write(word, avg);
	}
}
