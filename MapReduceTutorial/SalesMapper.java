package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	//every mappre class must based on MapReduceBase and imple Mapper interface

	//must have map() method

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString(); //value is the text in the csv
		String[] SingleCountryData = valueString.split(","); //"," is a delimiter
		output.collect(new Text(SingleCountryData[7]), one); // array[y],one

		// 7th in csv is "country" (from 0)
		// collect() method is a key-value pair of ouputcollector
	}
}
