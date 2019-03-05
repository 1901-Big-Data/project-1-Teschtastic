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
		
		for (DoubleWritable val: values) {
			newVal = val.get();
			
			//{[(V2-V1)/V1]/(time)} * 100.
		
			if(oldVal != 0.0 ) {
				increase = newVal - oldVal;
				perIncrease = (increase / oldVal) * 100.0;
		
				if(countryCode.equals("USA")) {
					switch(itemCode) {
						// gross primary school enrollment female
						case "SE.PRM.ENRR.FE":
							context.write(new Text(country+" primary"), new DoubleWritable(perIncrease));
							break;
							// gross secondary school enrollment female
						case "SE.SEC.ENRR.FE":
							context.write(new Text(country+" secondary"), new DoubleWritable(perIncrease));
							break;
							// gross tertiary school enrollment female
						case "SE.TER.ENRR.FE":
							context.write(new Text(country+" tertiary"), new DoubleWritable(perIncrease));
							break;
						default:
							break;
					}
				}
			}
			oldVal = newVal;
		}
	}
}