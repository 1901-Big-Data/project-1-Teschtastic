package com.revature.testing;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.Map;
import com.revature.reduce.Reduce;

public class Q5Test {
	private MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;
	private ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

	@Before
	public void setUp() {
		Map mapper = new Map();
		mapDriver = new MapDriver<LongWritable, Text, Text, DoubleWritable>();
		mapDriver.setMapper(mapper);
	
		Reduce reducer = new Reduce();
		reduceDriver = new ReduceDriver<Text, DoubleWritable, Text, DoubleWritable>();
		reduceDriver.setReducer(reducer);

		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testSimpleMapper() {
		mapDriver.withInput(new LongWritable(0), new Text("1,2,3,4,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,5.0,6.0,"));
		mapDriver.withOutput(new Text("1,4"), new DoubleWritable(5.0));
		mapDriver.withOutput(new Text("1,4"), new DoubleWritable(6.0));
		mapDriver.runTest();
	}
	
	@Test
	public void testEmptyCommaMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("1,2,3,4,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"));
		mapDriver.runTest();
	}

	@Test
	public void testSimpleReducer() {
		List<DoubleWritable> list = new ArrayList<DoubleWritable>();
		list.add(new DoubleWritable(20.0));
		list.add(new DoubleWritable(30.0));
		
		reduceDriver.withInput(new Text("United States,NY.GDP.MKTP.CD"), list);
		reduceDriver.withOutput(new Text("United States GDP growth average"), new DoubleWritable(25.0));
		reduceDriver.runTest();
	}

	@Test
	public void testMultipleReducer() {
		List<DoubleWritable> list = new ArrayList<DoubleWritable>();
		list.add(new DoubleWritable(30.0));
		list.add(new DoubleWritable(30.0));
		
		reduceDriver.withInput(new Text("United States,NY.GDP.MKTP.CD"), list);
		reduceDriver.withOutput(new Text("United States GDP growth average"), new DoubleWritable(30.0));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String input = "United States,USA,things and, stuff,NY.GDP.MKTP.CD,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,5.0,,10.0,,15.0,,";
		mapReduceDriver.addInput(new LongWritable(0), new Text(input));
		mapReduceDriver.addOutput(new Text("United States GDP growth average"), new DoubleWritable(10.0));
		
		mapReduceDriver.runTest();
	}

	@Test
	public void testRealMapReducerLarge() {
		String input = "United States,USA,GDP (current US$),NY.GDP.MKTP.CD,,,,,,,,,,,,38.56622,40.2428,42.38329,43.68353,46.55416,49.95025,50.68366,52.72272,53.23715,55.32357,58.25927,59.95962,60.32856,61.50404,61.95768,63.84709,67.49898,71.3865,75.14204,79.54622,81.83862,,88.99673,88.60509,89.10197,88.91812,,81.1478,82.92425,78.42035,79.51164,91.94142,94.7413,95.67165,96.80277,97.18494,98.19763,100.13819,104.22901,110.74234,113.03324,111.45427,103.65208,100.70205,99.59588,";
		mapReduceDriver.addInput(new LongWritable(0), new Text(input));
		mapReduceDriver.addOutput(new Text("United States GDP growth average"), new DoubleWritable(98.50117250000001));
	
		mapReduceDriver.runTest();
	}
}
