package com.opensam.service;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.opensam.driver.App;
import com.opensam.service.WordCountService.TokenizerMapper;

@Service
public class WordCount {

	final static Logger logger = Logger.getLogger(App.class);

	public void doWordCount(String input, String output) {
		System.out.println("Inside doWordCount");
		Configuration conf = new Configuration();
		Job job = null;
		try {
			job = Job.getInstance(conf, "word count");
		} catch (IOException e) {
			logger.error("", e);
			return;
		}
		job.setJarByClass(WordCountService.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		try {
			FileInputFormat.addInputPath(job, new Path(input));
		} catch (IllegalArgumentException | IOException e) {
			logger.error("", e);
			return;
		}
		FileOutputFormat.setOutputPath(job, new Path(output));
		try {
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			logger.error("", e);
		}
	}

}
