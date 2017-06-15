package com.opensam.service;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.opensam.vo.PairsVO;

public class PairsService {

	public static class PairsTokenizerMapper extends Mapper<Object, Text, PairsVO, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private PairsVO word = null;
		final static Logger logger = Logger.getLogger(PairsTokenizerMapper.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] inp = value.toString().split("\n", -1);
			int length = inp.length;
			for (int i = 0; i < length; i++) {
				String line = inp[i].trim();
				if (!StringUtils.isEmpty(line)) {
					String[] tokens = line.split(" ");
					int tokenLength = tokens.length;
					if (tokenLength > 1) {
						for (int k = 0; k < tokenLength - 1; k++) {
							String w = tokens[k];
							w = w.replaceAll("[^a-zA-Z]", "");
							if (!StringUtils.isBlank(w)) {
								for (int j = k + 1; j < tokenLength; j++) {
									String end = tokens[j];
									end = end.replaceAll("[^a-zA-Z]", "");
									if (!StringUtils.isBlank(end)) {
										word = new PairsVO(new Text(w + "," + end));
										context.write(word, one);
									}
								}
							}
						}
					}
				}
			}
		}
	}

	public static class PairsIntSumReducer extends Reducer<PairsVO, IntWritable, PairsVO, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(PairsVO key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			// System.out.println(key.toString()+","+result.toString());
			context.write(key, result);
		}
	}

}
