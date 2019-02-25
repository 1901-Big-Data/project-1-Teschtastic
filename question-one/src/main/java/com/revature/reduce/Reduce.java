package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		String desc = key.toString();
		String country = desc.substring(0, 3);
		String prob = desc.substring(4);

		double num = 0.0;
		for (DoubleWritable val: values) {
			num += val.get();
		}
		
		if(prob.equals("SE.TER.CUAT.BA.FE.ZS")) {
			context.write(new Text(country), new DoubleWritable(num));
		}
	}
}