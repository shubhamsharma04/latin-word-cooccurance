package com.opensam.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.stereotype.Service;

import com.opensam.service.ExhaustiveNGramService.ExhaustiveNGramMapper;
import com.opensam.service.ExhaustiveNGramService.ExhaustiveNGramReducer;
import com.opensam.vo.ExhPayloadVO;

@Service
public class ExhaustiveNGram {

	final static Log logger = LogFactory.getLog(ExhaustiveNGram.class);

	public void doExhaustiveNGramScaleUp(String input, String output, String localPath, String nGram, String lemmaPath,
			String numOfReducers) {
		logger.info("Inside doExhaustiveNGramScaleUp");
		logger.info("localPath : " + localPath);
		logger.info("lemmaPath : " + lemmaPath + " nGram : " + nGram);
		Collection<File> allFiles = FileUtils.listFiles(new File(localPath), null, false);
		int numOfFiles = allFiles.size();
		int numNGram = 0;
		int numReducers = 0;
		try {
			numNGram = Integer.parseInt(nGram);
			numReducers = Integer.parseInt(numOfReducers);
		} catch (NumberFormatException e2) {
			logger.error("Either Number of 'n' in Ngram : " + nGram + " is incorrect. or numReducers" + numOfReducers
					+ " Please rectify");
			return;
		}
		if (numOfFiles < 2 || numNGram < 1 || numReducers < 1) {
			logger.error(
					"Either the number of input files is less than 2 : " + numOfFiles + " OR Number of 'n' in Ngram : "
							+ nGram + " is incorrect. or numReducers" + numReducers + "  Will not proceed");
			return;
		}
		ExhaustiveNGramMapper.nGram = numNGram;
		try {
			populateLemmaMap(lemmaPath);
			copyFirstFile(input, localPath);
		} catch (IllegalArgumentException | IOException e1) {
			logger.error(e1);
			return;
		}

		for (int i = 1; i < numOfFiles; i++) {
			logger.info("Currently executing iteration : " + i);
			long currTime = System.currentTimeMillis();
			try {
				performTask(i, input, output + "_" + i, localPath, numReducers);
			} catch (IllegalArgumentException | IOException e) {
				logger.error(e);
			} catch (ClassNotFoundException e) {
				logger.error(e);
			} catch (InterruptedException e) {
				logger.error(e);
			}
			System.out.println((i + 1) + "," + (System.currentTimeMillis() - currTime));
		}

	}

	private void populateLemmaMap(String lemmaPath) throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(lemmaPath);
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream inputStream = fs.open(path);
		BufferedReader bufferedReader = new BufferedReader(
				new InputStreamReader(inputStream, StandardCharsets.UTF_8));
		String str = "";
		int count = 0;
		while ((str = bufferedReader.readLine()) != null) {
			count++;
			String[] keyVals = str.split(",");
			int length = keyVals.length;
			if (length >= 1) {
				String word = keyVals[0];
				Set<String> lemmaSet = new HashSet<String>();
				for (int i = 1; i < length; i++) {
					if (!StringUtils.isBlank(keyVals[i])) {
						lemmaSet.add(keyVals[i]);
					}
				}
				ExhaustiveNGramService.lemmaMap.put(word, lemmaSet);
			} else {
				logger.error("Invalid lemma : " + str);
			}

		}
		logger.info("Num of lemmas found : " + ExhaustiveNGramService.lemmaMap.size());
		logger.info("Total count : " + count);
	
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

	private void performTask(int i, String input, String output, String localPath, int numReducers)
			throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		logger.info("Inside performTask");
		Configuration conf = new Configuration();
		//conf.set("mapreduce.reduce.memory.mb", "-Xmx3072M");
		Job job = null;
		try {
			job = Job.getInstance(conf, "TrigramScaleUp");
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
		job.setNumReduceTasks(numReducers);
		job.setJarByClass(ExhaustiveNGramService.class);
		job.setMapperClass(ExhaustiveNGramMapper.class);
		// job.setCombinerClass(ExhaustiveNGramCombiner.class);
		job.setReducerClass(ExhaustiveNGramReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ExhPayloadVO.class);
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
	}

}
