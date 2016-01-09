package app.process.sentiment;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.hus.nlp.tokenizer.VietTokenizer;
import app.process.spellcheker.Checker;
import app.utils.dto.ListReportData;
import app.utils.dto.ReportData;
import app.utils.dto.StatusAndListComment;
import app.utils.spark.SparkUtil;

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
		tokenizer = new VietTokenizer();
	}

	public List<ListReportData> processLexiconSentiment(
			Map<String, List<String>> lstInputForSenti) {

		List<ListReportData> listPieData = new ArrayList<ListReportData>();

		// loop all of String input
		for (String status : lstInputForSenti.keySet()) {
			double totalScore = 0.0;
			// create pieData stored status
			ListReportData lstRP = new ListReportData();

			// get sentiScore of status
			double sentiStatus = 0;//runLexiconSentiment(status.toLowerCase());
			// TODO
			// increase scores of status
			totalScore += (sentiStatus * 1.5);

			// create ReportData stored data for status
			ReportData statusReport = new ReportData(
					getTypeOfColor(sentiStatus), status);

			// set status data to List return object
			lstRP.setStatusData(statusReport);

			List<ReportData> listCommentReport = new ArrayList<ReportData>();
			// loop for all comment
			for (String comments : lstInputForSenti.get(status)) {
				// sentiment value of comment
				double sentiComment = runLexiconSentiment(comments.toLowerCase());
				// sum of total
//				totalScore += sentiComment;

				// create comment report object
				ReportData commentReport = new ReportData(
						getTypeOfColor(sentiComment), comments);
				listCommentReport.add(commentReport);
			}

			// set list comment
			lstRP.setListCommentData(listCommentReport);

			// set total score
//			totalScore = totalScore / (lstInputForSenti.get(status).size() + 1);
			lstRP.setSentimentType(getTypeOfColor(totalScore));

			listPieData.add(lstRP);
		}

		return listPieData;
	}
	
	private double runLexiconSentiment(String inputText) {
		
		
		inputText = inputText.replaceAll(
				"[0-9\\<\\>\\|\\”\\“\\/\\\"\\:\\#\\)\\(\\%\\+]", "")
				.replaceAll("\\-", " ");
		System.out.println(inputText);
		double rs = 0.0;
		try {
			// First, we need to correct spelling and covert emoticons
			String correctSentence = correctSpellAndEmoticons(inputText);
			//correctSentence = correctSentence.replaceAll("[0-9]", REGEX_SPACE);
			String removeURL = replaceURLFromText(correctSentence);
			// Token each word in this sentence
			String[] rsCheckedAndToken = tokenizer.tokenize(removeURL);
			
			// Token each word in this sentence
			if (rsCheckedAndToken.length > 0) {
				// Calculate score of this sentence
				rs = VietSentiData.scoreTokens(rsCheckedAndToken[0]);
			}

		} catch (Exception ex) {
			logger.info(ex.getMessage());
			return rs;
		}

		return rs;
	}

	/**
	 * In this step, we need correct spelling and correct emoticons
	 * 
	 * @param inputText
	 *            String sentence input
	 * @return sentence after correct spell and emoticons
	 */
	private String correctSpellAndEmoticons(String inputText) {

		String emoticonsCorrect = Checker.correctEmoticons(inputText);
		String specialEmoticonsCorrect = Checker.correctSpecialEmoticons(emoticonsCorrect);
		String spellCorrect = Checker.correctSpell(specialEmoticonsCorrect);
		return spellCorrect;
	}
	
	/**
	 * Create Object after analysis sentiment and then send this object to
	 * client
	 * 
	 * @param lstInputForSenti
	 * @return Object for web client
	 */
	public List<ListReportData> processSentiment(
			Map<Integer, StatusAndListComment> lstInputForSenti) {

		List<ListReportData> listPieData = new ArrayList<ListReportData>();

		// loop all of String input
		for (Integer key : lstInputForSenti.keySet()) {
			// create pieData stored status
			ListReportData lstRP = new ListReportData();

			// get sentiScore of status
			StatusAndListComment item = new StatusAndListComment();
			item = lstInputForSenti.get(key);
			//qtran - remove .tolowercase()
			double sentiStatus = runAnalyzeSentiment(item.getStatus());

			// create ReportData stored data for status
			ReportData statusReport = new ReportData(
					getTypeOfColor(sentiStatus), item.getStatus());

			// set status data to List return object
			lstRP.setStatusData(statusReport);

			List<ReportData> listCommentReport = new ArrayList<ReportData>();
			// loop for all comment
			for (String comments : item.getListComment()) {
				// sentiment value of comment //qtran - remove .tolowercase()
				double sentiComment = runAnalyzeSentiment(comments);

				// create comment report object
				ReportData commentReport = new ReportData(
						getTypeOfColor(getTypeOfColor(sentiComment)), comments);
				listCommentReport.add(commentReport);
			}

			// set list comment
			lstRP.setListCommentData(listCommentReport);

			lstRP.setSentimentType(getTypeOfColor(sentiStatus));

			listPieData.add(lstRP);
		}

		return listPieData;
	}
	
	/**
	 * Get type of sentiment 0: neutral, 1: positive and -1: negative
	 * @param inputText
	 * @return
	 */
	private double runAnalyzeSentiment(String inputText) {

		double rs = 0.0;
		try {
			// First, we need to correct spelling and covert emoticons
			String correctSentence = correctSpellAndEmoticons(inputText);
			correctSentence = correctSentence.replaceAll("[0-9]", REGEX_SPACE);
			String removeURL = replaceURLFromText(correctSentence);
			// Token each word in this sentence
			if (removeURL.trim().length() > 0) {
				String[] rsCheckedAndToken = tokenizer.tokenize(removeURL);
				if (rsCheckedAndToken.length > 0) {
					// Calculate score of this sentence
					rs = ClassifySentiment.getClassifyOfSentiment(rsCheckedAndToken[0]);
				}
			}

		} catch (Exception ex) {
			logger.info(ex.getMessage());
			return rs;
		}

		return rs;
	}
	
	/**
	 * get type sentiment: POSITIVE, NEGATIVE or NEUTRAL base on sentiScore
	 * 
	 * @param sentiScore
	 * @return POSITIVE, NEGATIVE or NEUTRAL
	 */
	// TODO separates pie
	private int getTypeOfColor(double sentiScore) {
		if (sentiScore > 0) {
			return POSITIVE;
		} else if (sentiScore < 0) {
			return NEGATIVE;
		} else if (sentiScore == 0) {
			return NEUTRAL;
		}
		return NEUTRAL;
	}

	private static String replaceURLFromText(String input) {
		input = input.replaceAll("(https?|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*", " ");
		input = input.replace("\\", " ");
		input = input.replace("?", " ");
		input = input.replace("`", " ");
		input = input.replace("^", " ");
		input = input.replace("&", " ");
		input = input.replace("$", " ");
		input = input.replace("#", " ");
		return input;
	}

	public static void main(String[] args) {
		SparkUtil.createJavaSparkContext();
		Checker.init();
		VietSentiData.init();
		ClassifySentiment.createClassify();
		
		SentimentProcess stP = new SentimentProcess();
		Map<Integer, StatusAndListComment> lstInputForSenti = new LinkedHashMap<Integer, StatusAndListComment>();
		StatusAndListComment sttACm = new StatusAndListComment();
		
		try {
		    BufferedReader in = new BufferedReader(new FileReader("testData.txt"));
		    String str;
		    while ((str = in.readLine()) != null){
		    	sttACm.setStatus(str);
				
				List<String> cm = new ArrayList<String>();
				cm.add("nội dung");
				sttACm.setListComment(cm);
				
				lstInputForSenti.put(1, sttACm);
				
				stP.processSentiment(lstInputForSenti);
		    	
		    }
		    in.close();
		
		
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
