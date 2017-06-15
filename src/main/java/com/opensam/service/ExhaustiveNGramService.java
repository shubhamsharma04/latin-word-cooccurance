package com.opensam.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import com.opensam.vo.ExhPayloadVO;

public class ExhaustiveNGramService {
	
	public static Map<String, Set<String>> lemmaMap = new HashMap<String, Set<String>>();

	final static Log logger = LogFactory.getLog(ExhaustiveNGramService.class);

	public static class ExhaustiveNGramMapper extends Mapper<Object, Text, Text, ExhPayloadVO> {
		public static Integer nGram = 0;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			logger.info("Inside ExhaustiveNGramMapper");

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
					ExhPayloadVO exhPayloadVO = new ExhPayloadVO();
					exhPayloadVO.setLocation(new Text(left));
					exhPayloadVO.setnGram(new IntWritable(nGram));
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
					if (tokenLength >= nGram) {
						for (int j = 0; j < tokenLength - nGram + 1; j++) {
							String w = new String(tokens[j].replaceAll("[^a-zA-Z]", ""));
							if (!StringUtils.isBlank(w)) {
								if (!StringUtils.isBlank(w)) {
									String token = right.replaceFirst(w, "").trim();
									if (!StringUtils.isBlank(token)) {
										Text t = new Text(token.toLowerCase());
										exhPayloadVO.setValueText(t);
										context.write(new Text(w.toLowerCase()), exhPayloadVO);
									}
									right = token;
								}
							}
						}
					}

				}
			}
		}
	}
	
	public static class ExhaustiveNGramCombiner extends Reducer<Text, ExhPayloadVO, Text, List<ExhPayloadVO>> {
		
		public void reduce(Text key, Iterable<ExhPayloadVO> values, Context context)
				throws IOException, InterruptedException {
			List<ExhPayloadVO> output = new ArrayList<ExhPayloadVO>();
			for (ExhPayloadVO val : values) {
				output.add(val);
			}
			context.write(key, output);
		}
		
	}

	public static class ExhaustiveNGramReducer extends Reducer<Text, ExhPayloadVO, Text, Text> {

		public void reduce(Text key, Iterable<ExhPayloadVO> values, Context context)
				throws IOException, InterruptedException {
			logger.info("Inside ExhaustiveNGramReducer");
			long currTime = System.currentTimeMillis();
			Map<String, Set<String>> map = new HashMap<String, Set<String>>();
			for (ExhPayloadVO val : values) {
				String payload = val.toString();
				int nGram = Integer.parseInt(val.getnGram().toString());
				currTime = System.currentTimeMillis();
				if (nGram > 1) {
					String text = val.getValueText().toString();
					String[] in = text.split(" ");
					int length = in.length;
					Map<String, Integer> tMap = new HashMap<String, Integer>();
					if (length > 0) {
						for (String s : in) {
							s = s.replaceAll("[^a-zA-Z]", "");
							Set<String> lemmas = lemmaMap.get(s);
							if(lemmas==null){
								lemmas = new HashSet<String>();
								s = s.replaceAll("j", "i");
								s=s.replaceAll("v", "u");
								lemmas.add(s);
							}
							for (String lemma : lemmas) {
								if (tMap.containsKey(lemma)) {
									tMap.put(lemma, tMap.get(lemma) + 1);
								} else {
									tMap.put(lemma, 1);
								}
							}
						}
						Map<String, Integer> trMap = new TreeMap<String, Integer>();
						for (Entry<String, Integer> en : tMap.entrySet()) {
							trMap.put(en.getKey(), en.getValue());
						}

						List<String> words = new ArrayList<String>();
						List<Integer> counts = new ArrayList<Integer>();
						
						for (Entry<String, Integer> en : trMap.entrySet()) {
							words.add(en.getKey());
							counts.add(en.getValue());
						}
						logger.info("Total words : "+words.size());
						length = words.size();
						logger.info("Phase 1 finished. Time taken : "+(System.currentTimeMillis()-currTime));
						currTime = System.currentTimeMillis();
						if (length >= nGram - 1) {
							List<Integer> useLess = new ArrayList<Integer>();
							for (int i = 0; i < length; i++) {
								useLess.add(i);
							}
							
							// Credit : https://github.com/dpaukov/combinatoricslib
							ICombinatoricsVector<Integer> initialVector = Factory.createVector(useLess);
							Generator<Integer> gen = Factory.createSimpleCombinationGenerator(initialVector, nGram - 1);
							Iterator<ICombinatoricsVector<Integer>> iter = gen.iterator();
							// int count = 0;
							while (iter.hasNext()) {
								ICombinatoricsVector<Integer> it = iter.next();
								StringBuilder str = new StringBuilder();
								for (int i = 0; i < it.getSize(); i++) {
									String word = words.get(it.getValue(i));
									str.append(word).append(",");
								}
								if (str.length() > 0) {
									str.deleteCharAt(str.length() - 1);
								}
								String gram = str.toString();
								String[] toks = gram.split(",");
								StringBuilder tokStr = new StringBuilder();
								int len = toks.length;
								for (int k = len - 1; k >= 0; k--) {
									tokStr.append(toks[k]).append(",");
								} if(tokStr.length()>0){
									tokStr.deleteCharAt(tokStr.length()-1);
								}
								String revGram = tokStr.toString();
								if (map.containsKey(gram)) {
									Set<String> pos = map.get(gram);
									pos.add(payload);
								} else if(map.containsKey(revGram)){
									Set<String> revPos = map.get(revGram);
									revPos.add(payload);
								}else {
									Set<String> newPos = new HashSet<String>();
									newPos.add(payload);
									map.put(gram, newPos);
								}
							}
							logger.info("Phase 2 finished. Time taken : "+(System.currentTimeMillis()-currTime));
						}
					}
				} else {
					// WordCountCase
				}
				
			}
			currTime = System.currentTimeMillis();
			for (Entry<String, Set<String>> entry : map.entrySet()) {
				Set<String> list = entry.getValue();
				String gram = entry.getKey();
				String[] arr = new String[list.size()];
				list.toArray(arr);
				String keyString = key.toString();
				Set<String> lemmas = lemmaMap.get(keyString);

				if (lemmas == null) {
					lemmas = new HashSet<String>();
					keyString = keyString.replaceAll("j", "i");
					keyString = keyString.replaceAll("v", "u");
					lemmas.add(keyString);
				}
				for (String keyLemma : lemmas) {
					if (!StringUtils.isBlank(gram) && !StringUtils.isBlank(key.toString())) {
						context.write(new Text(keyLemma + "," + gram), new Text(Arrays.toString(arr)));
					}
				}

			}
			logger.info("Phase 3 finished. Time taken : "+(System.currentTimeMillis()-currTime));
			logger.info("Finished reduce");
		}
	}

}
