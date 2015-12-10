package app.process.sentiment;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.process.database.FBDatabaseProcess;
import app.process.spellcheker.Checker;
import app.utils.dto.ListReportData;
import app.utils.dto.ReportData;
import app.utils.spark.SparkUtil;
import vn.hus.nlp.tokenizer.VietTokenizer;

public class SentimentProcess {

	/**
	 * Neutral Sentiment
	 */
	private static final int NEUTRAL = 0;

	/**
	 * Negative Sentiment
	 */
	private static final int NEGATIVE = -1;

	/**
	 * Positive Sentiment
	 */
	private static final int POSITIVE = 1;

	private static final Logger logger = LoggerFactory
			.getLogger(SentimentProcess.class);
	
	/**
	 * String SPACE
	 */
	private static final String REGEX_SPACE = " ";
	
	private VietTokenizer tokenizer;
	
	public SentimentProcess() {
		tokenizer = new VietTokenizer("tokenizer.properties");
	}
	
	private double runAnalyzeSentiment(String inputText) {

		double rs = 0.0;
		try {
			// First, we need to correct spelling and covert emoticons
			String correctSentence = correctSpellAndEmoticons(inputText);
			// Token each word in this sentence
			String[] rsCheckedAndToken = tokenizer.tokenize(correctSentence);
			if (rsCheckedAndToken.length > 0) {
				// Calculate score of this sentence
				rs = VietSentiData.scoreTokens(rsCheckedAndToken[0].split(REGEX_SPACE));
			}

		} catch (Exception ex) {
			logger.info(ex.getMessage());
			return rs;
		}

		return rs;
	}
	
	private double runAnalyzeSentiment(String[] rsCheckedAndToken) {

		double rs = 0.0;
		try {
			// Token each word in this sentence
			if (rsCheckedAndToken.length > 0) {
				// Calculate score of this sentence
				rs = VietSentiData.scoreTokens(rsCheckedAndToken[0].split(REGEX_SPACE));
			}

		} catch (Exception ex) {
			logger.info(ex.getMessage());
			return rs;
		}

		return rs;
	}
	
	/**
	 * In this step, we need correct spelling and correct emoticons
	 * @param inputText String sentence input
	 * @return sentence after correct spell and emoticons
	 */
	private String correctSpellAndEmoticons(String inputText) {
		String spellCorrect = Checker.correctSpell(inputText);
		String emoticonsCorrect = Checker.correctEmoticons(spellCorrect);
		return emoticonsCorrect;
	}
	
	/**
	 * Create Object after analysis sentiment and then send this object to client
	 * @param lstInputForSenti
	 * @return Object for web client
	 */
	public List<ListReportData> processSentiment(Map<String, List<String>> lstInputForSenti) {
		
		List<ListReportData> listPieData = new ArrayList<ListReportData>();
		
		// loop all of String input
		for (String status : lstInputForSenti.keySet()) {
			double totalScore = 0.0;
			// create pieData stored status
			ListReportData lstRP = new ListReportData();
			
			// get sentiScore of status
			double sentiStatus = runAnalyzeSentiment(status);
			totalScore += sentiStatus;
			
			// create ReportData stored data for status
			ReportData statusReport = new ReportData(getTypeOfColor(sentiStatus), status);
			
			// set status data to List return object
			lstRP.setStatusData(statusReport);
			
			List<ReportData> listCommentReport = new ArrayList<ReportData>();
			// loop for all comment
			for (String comments : lstInputForSenti.get(status)) {
				// sentiment value of comment
				double sentiComment = runAnalyzeSentiment(comments);
				// sum of total
				totalScore += sentiComment;
				
				// create comment report object
				ReportData commentReport = new ReportData(getTypeOfColor(getTypeOfColor(sentiComment)), comments);
				listCommentReport.add(commentReport);
			}
			
			// set list comment
			lstRP.setListCommentData(listCommentReport);
			
			// set total score
			totalScore = totalScore / (lstInputForSenti.get(status).size() + 1);
			lstRP.setSentimentType(getTypeOfColor(totalScore));
			
			listPieData.add(lstRP);
		}
		
		return listPieData;
	}
	
	/**
	 * get type sentiment: POSITIVE, NEGATIVE or NEUTRAL base on sentiScore
	 * @param sentiScore
	 * @return POSITIVE, NEGATIVE or NEUTRAL
	 */
	private int getTypeOfColor(double sentiScore){
		if (sentiScore > 0) {
			return POSITIVE;
		} else if (sentiScore < 0) {
			return NEGATIVE;
		} else if (sentiScore == 0) {
			return NEUTRAL;
		}
		return NEUTRAL;
	}
	
	private static String replaceURLFromText(String input){
		input = input.replaceAll("[0-9]", "");
		input = input.replaceAll("(https?|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*", " ");
		return input;
	}
	
	private void writeData(List<String> listComment) {
		
		List<String> lstPositive = new ArrayList<String>();
		List<String> lstNegative = new ArrayList<String>();
		List<String> lstNeutral = new ArrayList<String>();
		
		for (String inputText : listComment) {
			
			inputText = replaceURLFromText(inputText);
			inputText = Checker.correctEmoticons(inputText);
			String[] rsCheckedAndToken = tokenizer.tokenize(inputText);
			double sentiScore = runAnalyzeSentiment(rsCheckedAndToken);
			if (sentiScore > 0) {
				lstPositive.add(rsCheckedAndToken[0]);
			} else if (sentiScore < 0) {
				lstNegative.add(rsCheckedAndToken[0]);
			} else if (sentiScore == 0) {
				lstNeutral.add(rsCheckedAndToken[0]);
			}
		}
		
		// write
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("POSITIVE.txt"), "utf-8"))) {

			for (String posi : lstPositive) {
				writer.write(posi + "\n");
			}
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("NEGATIVE.txt"), "utf-8"))) {

			for (String nega : lstNegative) {
				writer.write(nega + "\n");
			}
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("NEUTRAL.txt"), "utf-8"))) {

			for (String neu : lstNeutral) {
				writer.write(neu + "\n");
			}
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
	}
	
	public static void main(String[] args) {
		SparkUtil.createJavaSparkContext();
		Checker.init();
		VietSentiData.init();
		
		FBDatabaseProcess fbDt = new FBDatabaseProcess();
		List<String> cmData = fbDt.getCommentData();
		SentimentProcess stm = new SentimentProcess();
		stm.writeData(cmData);
	}
}
