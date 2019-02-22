package com.revature.testing;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
//import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
//import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Map;
//import com.revature.reduce.Reduce;

public class Q1Test {

	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;

	@Before
	public void setUp() {
		Map mapper = new Map();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);

	}
	
	@Test
	public void testSimpleMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("1,2,3,4,5.0,6.0"));
		mapDriver.withOutput(new Text("1 3"), new DoubleWritable(11.0));
		mapDriver.runTest();
	}
	@Test
	public void testEmptyCommaMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("1,2,3,4,,"));
		mapDriver.withOutput(new Text("1 3"), new DoubleWritable(0.0));
		mapDriver.runTest();
	}
}
