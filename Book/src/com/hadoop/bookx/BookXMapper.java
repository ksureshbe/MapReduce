package com.hadoop.bookx;

import java.io.IOException;
import java.lang.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text;

public class BookXMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{

  private final static IntWritable one = new IntWritable(1);
  private String[] singleBookDatas;

  public void map(LongWritable _key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	  String TempString = value.toString();
	  singleBookDatas = TempString.split("\";\"");
	  output.collect(new Text(), one);
	    
}

}
