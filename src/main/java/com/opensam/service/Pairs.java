package com.opensam.service;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.opensam.service.PairsService.PairsIntSumReducer;
import com.opensam.service.PairsService.PairsTokenizerMapper;
import com.opensam.vo.PairsVO;

@Service
public class Pairs {

	final static Logger logger = Logger.getLogger(Pairs.class);

	public void doPairsCooccur(String input, String output) {
		System.out.println("Inside doPairsCooccur");
		Configuration conf = new Configuration();
		Job job = null;
		try {
			job = Job.getInstance(conf, "Pairs Co-Occur");
		} catch (IOException e) {
			logger.error("", e);
			return;
		}
		job.setJarByClass(PairsService.class);
		job.setMapperClass(PairsTokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(PairsIntSumReducer.class);
		job.setOutputKeyClass(PairsVO.class);
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
