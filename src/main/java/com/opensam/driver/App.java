package com.opensam.driver;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.opensam.service.SimpleNGram;
import com.opensam.service.Lemma;
import com.opensam.service.Pairs;
import com.opensam.service.Stripes;
import com.opensam.service.ExhaustiveNGram;
import com.opensam.service.WordCount;
import com.opensam.util.GeneralConstants;

public class App {
	
	
	final static Logger logger = Logger.getLogger(App.class);

	public static void main(String[] args) {
		ApplicationContext context = new ClassPathXmlApplicationContext("Spring.xml");
		
		int action = 0;
		try {
			action = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {
			logger.error("",e);
			e.printStackTrace();
		}
		switch (action) {
		case GeneralConstants.ACTION_WORD_COUNT:
			WordCount wordCountService = (WordCount) context.getBean("wordCount");
			wordCountService.doWordCount(args[1],args[2]);
			break;
			
		case GeneralConstants.ACTION_PAIRS:
			Pairs pairs = (Pairs) context.getBean("pairs");
			pairs.doPairsCooccur(args[1],args[2]);
			break;
		case GeneralConstants.ACTION_STRIPES:
			Stripes stripes = (Stripes) context.getBean("stripes");
			stripes.doStripesCoOccur(args[1],args[2]);
			break;
			
		case GeneralConstants.ACTION_LEMMATIZE:
			Lemma lemma = (Lemma) context.getBean("lemma");
			lemma.doLemmatization(args[1],args[2],args[3]);
			break;
		case GeneralConstants.ACTION_DOSIMPLENGRAM:
			SimpleNGram biGram = (SimpleNGram) context.getBean("simpleNGram");
			biGram.doSimpleNGramScaleUp(args[1],args[2],args[3],args[4]);
			break;
		case GeneralConstants.ACTION_DOEXHAUSTIVENGRAM:
			ExhaustiveNGram triGram = (ExhaustiveNGram) context.getBean("exhaustiveNGram");
			triGram.doExhaustiveNGramScaleUp(args[1], args[2], args[3],args[4],args[5],args[6]);
			break;
		default:
			System.err.println("You entered : "+action+". Please enter a valid action");
			break;
		}
		
	}
}