package com.opensam.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LemmatisationService {
	final static Log logger = LogFactory.getLog(LemmatisationService.class);
	public static Map<String, Set<String>> lemmaMap = new HashMap<String, Set<String>>();

	public static class LemmatisationMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] inp = value.toString().split("\n", -1);
			int length = inp.length;
			for (int i = 0; i < length; i++) {
				String line = inp[i].trim();
				if (!StringUtils.isEmpty(line)) {
					String[] tokens = line.split(">");
					if (tokens.length != 2) {
						logger.warn("Something wrong with line : " + i);
						return;
					}
					String left = tokens[0].substring(1);
					String right = tokens[1].trim();
					tokens = right.split(" ");
					int tokenLength = tokens.length;
					Set<String> alreadyPresentKeys = new HashSet<String>();
					StringBuilder str = new StringBuilder();
					for (int k = 0; k < tokenLength; k++) {
						String tok = tokens[k];
						tok = tok.replaceAll("[^a-zA-Z]", "").toLowerCase().trim();
						if (!StringUtils.isBlank(tok)) {
							if (!alreadyPresentKeys.contains(tok)) {
								alreadyPresentKeys.add(tok);
								str.append(tok).append(" ");
							}
						}
					}
					if (str.length() > 0) {
						str.deleteCharAt(str.length() - 1);
					}
					logger.info("Previous number of keys : "+tokenLength);
					tokens = str.toString().split(" ");
					tokenLength = tokens.length;
					logger.info("Current number of keys : "+tokenLength);
					if (tokenLength >0) {
						for (int j = 0; j < tokenLength; j++) {
							String w = new String(tokens[j].replaceAll("[^a-zA-Z]", "")).toLowerCase();
							if (!StringUtils.isBlank(w)) {
								Set<String> allLemmas = LemmatisationService.lemmaMap.get(w);
								if (allLemmas == null) {
									allLemmas = new HashSet<String>();
									String word= w.replaceAll("j", "i");
									word = w.replaceAll("v", "u");
									allLemmas.add(word);
								}
								for (String lem : allLemmas) {
									context.write(new Text(lem),new Text( "{ "+ w +" "+left+" }"));
								}
							}
						}
						}
					}
				}
			}
		}

	public static class LemmatisationReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			logger.info("Inside Reducer");
			StringBuilder str = new StringBuilder();
			while (values.iterator().hasNext()) {
				str.append(values.iterator().next().toString()).append(" ");
			}
			context.write(key, new Text(str.toString().trim()));
		}
	}

}
