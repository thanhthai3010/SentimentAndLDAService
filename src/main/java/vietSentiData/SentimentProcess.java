package vietSentiData;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	public static void main(String[] args) {
		SparkUtil.createJavaSparkContext();
		Checker.init();
		VietSentiData.init();
		
		String a = StringEscapeUtils.escapeJava("😀");
		System.out.println(a);
		
		SentimentProcess smP = new SentimentProcess();
		Map<String, List<String>> sttAndCm = new LinkedHashMap<String, List<String>>();
		
		sttAndCm.put("status1 :'( lắm mà", new ArrayList<String>(){{
			add(":( of status 1");
		}});
		
		sttAndCm.put("hôm nay tôi rất là vui", new ArrayList<String>(){{
			add("chuyện này =)) quá of status 2");
		}});
		
		List<ListReportData> rs = smP.processSentiment(sttAndCm);
		
		System.out.println(ListReportData.toJson(rs));
		
//		for (ListReportData ite : rs) {
//			System.out.println("score: " + ite.getSentimentType());
//			System.out.println("---------------");
//			System.out.println(" status: " + ite.getStatusData().getContentData());
//			System.out.println("---------------");
//			for (ReportData listReportData : ite.getListCommentData()) {
//				System.out.println("comment: " + listReportData.getContentData());
//				System.out.println("---------------");
//			}
//		}
	}
}
