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

public class Q4Test {
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
		mapDriver.withOutput(new Text("1,2,4"), new DoubleWritable(5.0));
		mapDriver.withOutput(new Text("1,2,4"), new DoubleWritable(6.0));
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
		list.add(new DoubleWritable(25.0));
		list.add(new DoubleWritable(30.0));
		
		reduceDriver.withInput(new Text("United States,USA,SL.AGR.EMPL.FE.ZS"), list);
		reduceDriver.withOutput(new Text("United States agricultural"), new DoubleWritable(20.0));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() {
		String input = "United States,USA,things and, stuff,SL.AGR.EMPL.FE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,5.0,,10.0,,15.0,,";
		mapReduceDriver.addInput(new LongWritable(0), new Text(input));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(100.0));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(50.0));
		
		mapReduceDriver.runTest();
	}

	@Test
	public void testRealMapReducerLarge() {
		String input = "United States,USA,Employment in agriculture, female (% of male employment),SL.AGR.EMPL.FE.ZS,10.1899995803833,9.85000038146973,9.21000003814697,8.52999973297119,8.11999988555908,7.65000009536743,6.90999984741211,6.65999984741211,6.55999994277954,6.07000017166138,5.84000015258789,5.65999984741211,5.59999990463257,5.44000005722046,5.51000022888184,5.44999980926514,5.15999984741211,4.88000011444092,4.80999994277954,4.65999984741211,4.73999977111816,4.69999980926514,4.8600001335144,4.76000022888184,4.51999998092651,4.23000001907349,4.11999988555908,4.09000015258789,3.94000005722046,3.91000008583069,3.91000008583069,4.03000020980835,4,3.78999996185303,3.83999991416931,3.79999995231628,3.76999998092651,3.66000008583069,3.60999989509583,3.40000009536743,2.53999996185303,2.32999992370605,2.35999989509583,2.30999994277954,2.25999999046326,2.1800000667572,2.15000009536743,2.04999995231628,2.13000011444092,2.1800000667572,2.26999998092651,2.28999996185303,2.15000009536743,2.10999989509583,2.17000007629395,2.30999994277954,2.27999997138977";
		mapReduceDriver.addInput(new LongWritable(0), new Text(input));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-8.267718161451345));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(1.287552462322099));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-2.118642141476013));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-2.1645001538881874));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-3.5398196479488235));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-1.3761454344538488));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-4.651169237927865));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(3.9024470236815567));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(2.3474154755810264));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(4.128436303361546));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(0.8810564358840571));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-6.113531389420398));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-1.8604743487122426));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(2.8436106247007693));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(6.451606523659192));
		mapReduceDriver.addOutput(new Text("United States agricultural"), new DoubleWritable(-1.298700092332997));
	
		mapReduceDriver.runTest();
	}
}
