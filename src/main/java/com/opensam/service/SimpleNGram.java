package com.opensam.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Service;

import com.opensam.service.SimpleNGramService.SimpleNGramMapper;
import com.opensam.service.SimpleNGramService.SimpleNGramReducer;
import com.opensam.vo.NGramPayLoadVO;

@Service
public class SimpleNGram {

	final static Log logger = LogFactory.getLog(SimpleNGram.class);

	public void doSimpleNGramScaleUp(String input, String output, String localPath, String nGram) {
		logger.info("Inside doBigramScaleUp");
		logger.info("localPath : " + localPath);
		Collection<File> allFiles = FileUtils.listFiles(new File(localPath), null, false);
		int numOfFiles = allFiles.size();
		int numNGram = 0;
		try {
			numNGram = Integer.parseInt(nGram);
		} catch (NumberFormatException e2) {
			logger.error("Number of 'n' in Ngram : "+nGram+" is incorrect. Please rectify");
			return;
		}
		if (numOfFiles < 2 || numNGram < 1) {
			logger.error("Either the number of input files is less than 2 : " + numOfFiles + " OR Number of 'n' in Ngram : "+nGram +" is incorrect.  Will not proceed");
			return;
		}

		SimpleNGramMapper.nGram = numNGram;
		//BiGramMapper.lemmaPath = lemmaPath;
		try {
			copyFirstFile(input, localPath);
		} catch (IllegalArgumentException | IOException e1) {
			logger.error(e1);
			return;
		}

		for (int i = 1; i < numOfFiles; i++) {
			logger.info("Currently executing iteration : " + i);
			long currTime = System.currentTimeMillis();
			try {
				performTask(i, input, output + "_" + i, localPath);
			} catch (IllegalArgumentException | IOException e) {
				logger.error(e);
			} catch (ClassNotFoundException e) {
				logger.error(e);
			} catch (InterruptedException e) {
				logger.error(e);
			}
			System.out.println(i + "," + (System.currentTimeMillis() - currTime)	);
		}
	}

	private void copyFirstFile(String input, String localPath) throws IllegalArgumentException, IOException {
		logger.info("Inside copyFirstFile");
		Collection<File> allFiles = FileUtils.listFiles(new File(localPath), null, false);
		List<File> files = new ArrayList<File>(allFiles);
		Path path = new Path(input);
		FileSystem fs = path.getFileSystem(new Configuration());
		fs.delete(new Path(input), true);
		fs.mkdirs(new Path(input));
		fs.copyFromLocalFile(false, true, new Path(localPath + "/" + files.get(0).getName()), new Path(input));
	}

	private void performTask(int i, String input, String output, String localPath)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		logger.info("Inside performTask");
		Configuration conf = new Configuration();
		Job job = null;
		try {
			job = Job.getInstance(conf, "BigramScaleUp");
		} catch (IOException e) {
			logger.error("", e);
			return;
		}

		Collection<File> allFiles = FileUtils.listFiles(new File(localPath), null, false);
		List<File> files = new ArrayList<File>(allFiles);
		Path path = new Path(input);
		FileSystem fs = path.getFileSystem(job.getConfiguration());
		logger.info("This time copying file : " + localPath + "/" + files.get(i).getName());
		fs.copyFromLocalFile(false, true, new Path(localPath + "/" + files.get(i).getName()), new Path(input));
		job.setNumReduceTasks(175);
		job.setJarByClass(SimpleNGramService.class);
		job.setMapperClass(SimpleNGramMapper.class);
		job.setReducerClass(SimpleNGramReducer.class);
		job.setMapOutputValueClass(NGramPayLoadVO.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		try {
			FileInputFormat.addInputPath(job, new Path(input));
		} catch (IllegalArgumentException | IOException e) {
			logger.error("", e);
			return;
		}
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		/*
		 * try { System.exit(job.waitForCompletion(true) ? 0 : 1); } catch
		 * (ClassNotFoundException | IOException | InterruptedException e) {
		 * logger.error("", e); }
		 */
	}

}
