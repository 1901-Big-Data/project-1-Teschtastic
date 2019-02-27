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
		double max = 0.0;
		double min = 500.0;
		double increase = 0.0;
		double count = 0.0;
		
		for(String code: desc) {
			if(code.equals(desc[0]))
				country = code;
			else if(code.equals(desc[1]))
				countryCode = code;
			else if(code.equals(desc[2]))
				itemCode = code;
		}
		
		for (DoubleWritable val: values) {
			if(val.get() > max)
				max = val.get();
			if(val.get() < min || val.get() < max)
				min = val.get();
			count++;
		}
		
		if(count == 0.0)
			count = 1.0;
		
		increase = max - min;
		increase /= count;
		
		if(countryCode.equals("USA")) {
			switch(itemCode) {
				// gross primary school enrollment female
				case "SE.PRM.ENRR.FE":
					context.write(new Text(country+","+itemCode), new DoubleWritable(increase));
					break;
				// gross secondary school enrollment female
				case "SE.SEC.ENRR.FE":
					context.write(new Text(country+","+itemCode), new DoubleWritable(increase));
					break;
				// gross tertiary school enrollment female
				case "SE.TER.ENRR.FE":
					context.write(new Text(country+","+itemCode), new DoubleWritable(increase));
					break;
				default:
					break;
			}
		}
	}
}