package com.opensam.service;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.opensam.service.StripesService.StripesTokenizerMapper;
import com.opensam.service.StripesService.StripesTokenizerMapper.StripesIntSumReducer;

@Service
public class Stripes {
	final static Logger logger = Logger.getLogger(Stripes.class);

	public void doStripesCoOccur(String input, String output) {
		System.out.println("Inside doStripesCoOccur");
		Configuration conf = new Configuration();
		Job job = null;
		try {
			job = Job.getInstance(conf, "Stripes Co-Occur");
		} catch (IOException e) {
			logger.error("", e);
			return;
		}
		job.setJarByClass(StripesService.class);
		job.setMapperClass(StripesTokenizerMapper.class);
		job.setReducerClass(StripesIntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputValueClass(Text.class);
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
