package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	static volatile private ArrayList<String> headers = new ArrayList<String>();
	static volatile private String[] years = {"1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016"};
	
	{
	String[] input = {"Country Name","Country Code","Indicator Name","Indicator Code","1960","1961","1962","1963","1964","1965","1966","1967","1968","1969","1970","1971","1972","1973","1974","1975","1976","1977","1978","1979","1980","1981","1982","1983","1984","1985","1986","1987","1988","1989","1990","1991","1992","1993","1994","1995","1996","1997","1998","1999","2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015","2016"};	
		
		for(String header: input) {
			headers.add(header);
		}
	}
	
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		line = line.replace(", ", "");
		line = line.replace("\"", "");
		
		if(line.contains("Country Name"))
			return;
		
		String country = new String();
		String countryCode = new String();
		String itemCode = new String();
		int yearLocation;
		double rate;
		
		String[] data = line.split(",");
		
		for(String code: data) {
			if(code == data[0])
				country = code;
			else if(code == data[1])
				countryCode = code;
			else if(code == data[3])
				itemCode = code;
		}
		
		for(String year: years) {
			try {
				yearLocation = headers.indexOf(year);
				
				if (yearLocation >= data.length)
					return;
				
				rate = Double.parseDouble(data[yearLocation]);

				if (rate > 0.0)
					context.write(new Text(country+","+countryCode+","+itemCode), new DoubleWritable(rate));
			} catch(NumberFormatException e) {
				
			}
		}
	}
}
