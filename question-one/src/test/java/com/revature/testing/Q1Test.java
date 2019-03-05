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

public class Q1Test {

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
		mapDriver.withInput(new LongWritable(1), new Text("1,2,3,4,5.0,6.0"));
		mapDriver.withOutput(new Text("1,2,4"), new DoubleWritable(5.0));
		mapDriver.withOutput(new Text("1,2,4"), new DoubleWritable(6.0));
		mapDriver.runTest();
	}
	
	@Test
	public void testEmptyCommaMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("1,2,3,4,,"));
		mapDriver.runTest();
	}
	
	@Test
	public void testSimpleReducer() {
		List<DoubleWritable> list = new ArrayList<DoubleWritable>();
		list.add(new DoubleWritable(25.0));
		
		reduceDriver.withInput(new Text("Arab World,stuff,SE.TER.HIAT.BA.FE.ZS"), list);
		reduceDriver.withOutput(new Text("Arab World Bachelor's:"), new DoubleWritable(25.0));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMultipleReducer() {
		List<DoubleWritable> list = new ArrayList<DoubleWritable>();
		list.add(new DoubleWritable(25.0));
		list.add(new DoubleWritable(25.0));
		
		reduceDriver.withInput(new Text("Arab World,stuff,SE.TER.HIAT.BA.FE.ZS"), list);
		reduceDriver.withOutput(new Text("Arab World Bachelor's:"), new DoubleWritable(25.0));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMultipleReducerLarge() {
		List<DoubleWritable> list = new ArrayList<DoubleWritable>();
		list.add(new DoubleWritable(31.0));
		list.add(new DoubleWritable(33.0));
		
		reduceDriver.withInput(new Text("Arab World,ARB,SE.TER.HIAT.BA.FE.ZS"), list);
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String input = "Arab World,stuff, and thing,ARB,SE.TER.HIAT.BA.FE.ZS,12.0,14.0";
		mapReduceDriver.addInput(new LongWritable(0), new Text(input));
		mapReduceDriver.addOutput(new Text("Arab World Bachelor's:"), new DoubleWritable(13.0));
		
		mapReduceDriver.runTest();
	}
	
	@Test
	public void testRealMapReducer() {
		String in = "Arab World,ARB,Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%),SE.TER.HIAT.BA.FE.ZS,12.0,14.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,";
		
		mapReduceDriver.addInput(new LongWritable(0), new Text(in));
		mapReduceDriver.addOutput(new Text("Arab World Bachelor's:"), new DoubleWritable(13.0));
		mapReduceDriver.runTest();
	}
	
	@Test
	public void testRealMapReducerLarge() {
		String in = "Arab World,Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%),SE.TER.HIAT.BA.FE.ZS,31.0,33.0,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,";
		
		mapReduceDriver.addInput(new LongWritable(0), new Text(in));
		mapReduceDriver.runTest();
	}
}
