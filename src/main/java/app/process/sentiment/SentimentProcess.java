package app.process.sentiment;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.hus.nlp.tokenizer.VietTokenizer;
import app.process.lda.Stopwords;
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

	private static final Logger logger = LoggerFactory.getLogger(SentimentProcess.class);

	/**
	 * String SPACE
	 */
	private static final String REGEX_SPACE = " ";

	private VietTokenizer tokenizer;

	public SentimentProcess() {
		tokenizer = new VietTokenizer();
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
					getTypeOfColor(sentiStatus), removeHTMLTags(item.getStatus()));

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

	/**
	 * Replace URL and some special characters from String input
	 * @param input
	 * @return
	 */
	private String replaceURLFromText(String input) {
		input = input.replaceAll("(https?|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*", " ");
		input = input.replace("\\", " ");
		input = input.replace("!", " ");
		input = input.replace("?", " ");
		input = input.replace("`", " ");
		input = input.replace("^", " ");
		input = input.replace("&", " ");
		input = input.replace("$", " ");
		input = input.replace("*", " ");
		input = input.replace("(", " ");
		input = input.replace(")", " ");
		input = input.replace("+", " ");
		input = input.replace("=", " ");
		input = input.replace("#", " ");
		input = input.replace("<", " ");
		input = input.replace(">", " ");
		input = input.replace("~", " ");
		return input;
	}

	/**
	 * Remove some HTML Tags
	 * @param input
	 * @return String output
	 */
	private String removeHTMLTags(String input) {
		input = input.replace("<", " ");
		input = input.replace(">", " ");
		return input;
	}
	
	public static void main(String[] args) {
		SparkUtil.createJavaSparkContext();
		Checker.init();
		Stopwords.init();
		ClassifySentiment.createClassify();
		
		SentimentProcess stP = new SentimentProcess();
		Map<Integer, StatusAndListComment> lstInputForSenti = new LinkedHashMap<Integer, StatusAndListComment>();
		StatusAndListComment sttACm = new StatusAndListComment();
		sttACm.setStatus("BẠN NAM HỎI BÀI MÌNH Ạ.. THẾ NHÉ   >>>.<<<<OMG.. mình hơi nóng tính ... đầu tháng đừng gạch đá mình nhé.. các bạn..thân ái chào tạm biệt và không hẹn gặp lại ở CON PHÉT SỪN nữa đâu nhá..Thật là VI DIỆU mè.");
		List<String> listComment = new ArrayList<String>();
//		JavaRDD<String> listInput = SparkUtil.getJavaSparkContext().textFile("./DictionaryData/listInput.txt");
		
//		for (String item : listInput.collect()) {
//			listComment.add(item);
//		}
		listComment.add("khoái trá vui vẻ");
		
		sttACm.setListComment(listComment);
		lstInputForSenti.put(1, sttACm);
		stP.processSentiment(lstInputForSenti);
	}
}
