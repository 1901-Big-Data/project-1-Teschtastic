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
		double oldVal = 0.0;
		double newVal = 0.0;
		double change = 0.0;
		double perChange = 0.0;
		
		for(String code: desc) {
			if(code.equals(desc[0]))
				country = code;
			else if(code.equals(desc[2]))
				itemCode = code;
		}
		
		for (DoubleWritable val: values) {
			newVal = val.get();
			
			//{[(V2-V1)/V1]/(time)} * 100.
		
			if(oldVal != 0.0 ) {
				change = newVal - oldVal;
				perChange = (change / oldVal) * 100.0;
		
				switch(itemCode) {
				// gross agricultural male employment
				case "SL.AGR.EMPL.FE.ZS":
					context.write(new Text(country+" agricultural"), new DoubleWritable(perChange));
					break;
					// gross industrial male employment
				case "SL.IND.EMPL.FE.ZS":
					context.write(new Text(country+" industrial"), new DoubleWritable(perChange));
					break;
					// gross services male employment
				case "SL.SRV.EMPL.FE.ZS":
					context.write(new Text(country+" service"), new DoubleWritable(perChange));
					break;
				default:
					break;
				}
			}
			oldVal = newVal;
		}
	}
}