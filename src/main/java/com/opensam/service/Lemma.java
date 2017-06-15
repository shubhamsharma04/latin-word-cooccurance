package com.opensam.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

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

import com.opensam.service.LemmatisationService.LemmatisationMapper;
import com.opensam.service.LemmatisationService.LemmatisationReducer;

@Service
public class Lemma {
	final static Log logger = LogFactory.getLog(Lemma.class);

	public void doLemmatization(String input, String output, String lemmaPath) {

		logger.info("Inside doLemmatization");
		Configuration conf = new Configuration();
		Job job = null;
		try {
			job = Job.getInstance(conf, "Lemmatisation");
		} catch (IOException e) {
			logger.error("", e);
			return;
		}
		try {
			populateLemmaMap(lemmaPath);
		} catch (IOException e1) {
			logger.error(e1);
		}
		job.setJarByClass(LemmatisationService.class);
		job.setMapperClass(LemmatisationMapper.class);
		job.setReducerClass(LemmatisationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
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
				LemmatisationService.lemmaMap.put(word, lemmaSet);
			} else {
				logger.error("Invalid lemma : " + str);
			}

		}
		logger.info("Num of lemmas found : " + LemmatisationService.lemmaMap.size());
		logger.info("Total count : " + count);
	}
	

	
	
}
