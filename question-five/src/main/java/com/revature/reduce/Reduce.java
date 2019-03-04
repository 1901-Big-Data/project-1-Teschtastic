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
		String itemCode = new String();
		double num = 0.0;
		double count = 0.0;
		double avg = 0.0;
		
		for(String code: desc) {
			if(code.equals(desc[0]))
				country = code;
			else if(code.equals(desc[1]))
				itemCode = code;
		}
		
		for (DoubleWritable val: values) {
			num += val.get();
			count++;
		}
		
		if(count < 1.0)
			count = 1.0;
		
		avg = num / count;
		
		if(itemCode.equals("NY.GDP.MKTP.CD")) {
			context.write(new Text(country+" GDP growth average"), new DoubleWritable(avg));
		}
	}
}