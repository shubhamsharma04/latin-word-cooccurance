package com.opensam.service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class StripesService {

	public static class StripesTokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {

		private Text word = new Text();
		final static Logger logger = Logger.getLogger(StripesTokenizerMapper.class);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] inp = value.toString().split("\n", -1);
			int length = inp.length;
			for (int i = 0; i < length; i++) {
				String line = inp[i].trim();
				MapWritable mapWritable = new MapWritable();
				if (!StringUtils.isEmpty(line)) {
					String[] tokens = line.split(" ");
					int tokenLength = tokens.length;
					if (tokenLength > 1) {
						for (int k = 0; k < tokenLength - 1; k++) {
							String w = tokens[k];
							w = w.replaceAll("[^a-zA-Z]", "");
							word = new Text(w);
							if (!StringUtils.isBlank(w)) {
								for (int j = k + 1; j < tokenLength; j++) {
									String end = tokens[j];
									end = end.replaceAll("[^a-zA-Z]", "");
									if (!StringUtils.isBlank(end)) {
										Text t = new Text(end);
										if (mapWritable.containsKey(t)) {
											mapWritable.put(t, new Text(String
													.valueOf(Integer.valueOf(mapWritable.get(t).toString()) + 1)));
										} else {
											mapWritable.put(t, new Text(String.valueOf(1)));
										}
									}
									context.write(word, mapWritable);
								}
							}
						}
					}
				}
			}

		}

		public static class StripesIntSumReducer extends Reducer<Text, MapWritable, Text, Text> {
			public void reduce(Text key, Iterable<MapWritable> values, Context context)
					throws IOException, InterruptedException {
				Map<String, Integer> map = new HashMap<String, Integer>();
				for (MapWritable mapWritable : values) {
					for (java.util.Map.Entry<Writable, Writable> extractData : mapWritable.entrySet()) {
						String neighbour = String.valueOf(extractData.getKey());
						int value = Integer.valueOf(String.valueOf(extractData.getValue()));
						if (map.containsKey(key)) {
							map.put(neighbour, map.get(neighbour) + value);
						} else {
							map.put(neighbour, value);
						}
					}
				}
				StringBuilder str = new StringBuilder();
				str.append("[");
				for (Entry<String, Integer> entry : map.entrySet()) {
					str.append(entry.getKey()).append(":").append(entry.getValue()).append("  ");
				}
				str.append("]");
				//for (java.util.Map.Entry<String, Integer> entry : map.entrySet()) {
					//String output = new String(key.toString() + "," + entry.getKey());
					// System.out.println("Reduced word : " + output + " with
					// value : " + entry.getValue());
					context.write(key, new Text(str.toString()));
				//}
			}
		}

	}

}
