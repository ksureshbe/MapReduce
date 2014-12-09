package com.hadoop.bookx;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.File;


public class BookXDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if(args.length!=2){
            System.out.println("Error");

            System.exit(-1);
        }
        @SuppressWarnings("deprecation")
		Job job=new Job();
        job.setJarByClass(com.hadoop.bookx.BookXDriver.class);
        job.setMapperClass(com.hadoop.bookx.BookXMapper.class); 
        job.setReducerClass(com.hadoop.bookx.BookxReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class); /*Reducer Output Key and value class*/
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(CustomInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        boolean success=job.waitForCompletion(true);
        System.exit(success?0:-1);
    }


}
