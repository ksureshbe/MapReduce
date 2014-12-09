package com.hadoop.bookx;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text;

public class BookxReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text _key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		Text key = _key;
        int frequencyForYear = 0;
        
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			IntWritable value = (IntWritable) values.next();
			frequencyForYear += value.get();
		}
	   output.collect(key, new IntWritable(frequencyForYear));
	}

}
