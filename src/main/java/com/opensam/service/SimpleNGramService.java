package com.opensam.service;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.opensam.vo.NGramPayLoadVO;
import com.opensam.vo.PairsVO;

public class SimpleNGramService {
	final static Log logger = LogFactory.getLog(SimpleNGramService.class);

	public static class SimpleNGramMapper extends Mapper<Object, Text, Text, NGramPayLoadVO> {
		public static Integer nGram = 0;

	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			logger.info("Inside NGramMapper with N in nGram as : " + nGram);
			String[] inp = value.toString().split("\n", -1);
			int length = inp.length;
			for (int i = 0; i < length; i++) {
				String line = inp[i].trim();
				if (!StringUtils.isEmpty(line)) {
					String[] input = line.split(">");
					if (input.length == 2) {
						String left = input[0].substring(1);
						String right = input[1].trim();
						input = left.split("\\.");
						int lengthMeta = input.length;
						if (lengthMeta == 3 || lengthMeta == 4) {
							NGramPayLoadVO nGramPayLoadVO = new NGramPayLoadVO();
							int chapNo = 0;
							int lineNum = 0;
							String author = input[0];
							String docName = input[1];
							if (lengthMeta == 4) {
								try {
									chapNo = Integer.parseInt(input[2].trim());
								} catch (NumberFormatException e) {
									logger.error(e);
								}
							}
							try {
								lineNum = Integer.parseInt(input[lengthMeta - 1].trim());
							} catch (NumberFormatException e) {
								logger.error(e);
							}
							nGramPayLoadVO.setAuthor(new Text(author));
							nGramPayLoadVO.setDocName(new Text(docName));
							nGramPayLoadVO.setChapNo(new IntWritable(chapNo));
							nGramPayLoadVO.setLineNum(new IntWritable(lineNum));
							int position = 0;
							String[] tokens = right.split(" ");
							int tokenLength = tokens.length;
							if (tokenLength >= nGram) {
								for (int j = 0; j < tokenLength - nGram + 1; j++) {
									StringBuilder w = new StringBuilder(tokens[j].replaceAll("[^a-zA-Z]", ""));
									if (w.length() > 0) {
										position = j;
										for (int k = j + 1; k < j + nGram; k++) {
											String trail = tokens[k].trim();
											trail = trail.replaceAll("[^a-zA-Z]", "");
											if (trail.length() > 0) {
												w.append(",").append(trail);
												position = k;
											} else {
												logger.warn("Size of NGram might reduce");
											}
										}

										nGramPayLoadVO.setPosition(new IntWritable(position));
										context.write(new Text(w.toString()), nGramPayLoadVO);
									}
								}
							}
						}
					}
				}
			}

		}
	}

	public static class SimpleNGramReducer extends Reducer<Text, NGramPayLoadVO, Text, Text> {

		public void reduce(PairsVO key, Iterable<NGramPayLoadVO> values, Context context)
				throws IOException, InterruptedException {
			logger.info("Inside BiGramReducer");
			StringBuilder output = new StringBuilder();
			output.append("{");
			for (NGramPayLoadVO payLoad : values) {
				output.append("[ ");
				output.append(payLoad.toString());
				output.append(" ]");
			}
			output.append("}");
			context.write(key.getText(), new Text(output.toString()));
		}
	}

}
