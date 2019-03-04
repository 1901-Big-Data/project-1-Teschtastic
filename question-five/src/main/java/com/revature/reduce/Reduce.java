package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] desc = key.toString().split(",");
		String country = new String();
		String countryCode = new String();
		String itemCode = new String();
		double oldVal = 0.0;
		double newVal = 0.0;
		double increase = 0.0;
		double perIncrease = 0.0;
		
		
		for(String code: desc) {
			if(code.equals(desc[0]))
				country = code;
			else if(code.equals(desc[1]))
				countryCode = code;
			else if(code.equals(desc[2]))
				itemCode = code;
		}
	}
}