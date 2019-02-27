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
		double max = 0.0;
		double min = 101.0;
		double increase;
		
		for(String code: desc) {
			if(code.equals(desc[0]))
				country = code;
			else if(code.equals(desc[2]))
				itemCode = code;
		}
		
		for (DoubleWritable val: values) {
			if(val.get() > max)
				max = val.get();
			if(val.get() < min || val.get() < max)
				min = val.get();
		}
		
		increase = max - min;
		
		switch(itemCode) {
			// gross agricultural female employment
			case "SL.AGR.EMPL.FE.ZS":
				context.write(new Text(country+","+itemCode), new DoubleWritable(increase));
				break;
			// gross industrial female employment
			case "SL.IND.EMPL.FE.ZS":
				context.write(new Text(country+","+itemCode), new DoubleWritable(increase));
				break;
			// gross services female employment
			case "SL.SRV.EMPL.FE.ZS":
				context.write(new Text(country+","+itemCode), new DoubleWritable(increase));
				break;
			default:
				break;
		}
	}
}