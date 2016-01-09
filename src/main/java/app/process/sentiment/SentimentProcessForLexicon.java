/**
 * 
 */
package app.process.sentiment;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import main.ExtractOpinion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.hus.nlp.tokenizer.VietTokenizer;
import app.process.database.FBDatabaseProcess;
import app.process.spellcheker.Checker;
import app.utils.dto.FacebookData;
import app.utils.dto.ListReportData;
import app.utils.dto.ReportData;
import app.utils.dto.StatusAndListComment;
import app.utils.spark.SparkUtil;

/**
 * @author qtran
 *
 */
public class SentimentProcessForLexicon {

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
			.getLogger(SentimentProcessForLexicon.class);

	/**
	 * String SPACE
	 */
	private static final String REGEX_SPACE = " ";
	
	private List<ListReportData> listPieData =  Collections.synchronizedList(new ArrayList<ListReportData>());

	private VietTokenizer tokenizer;

	public SentimentProcessForLexicon() {
		tokenizer = new VietTokenizer();
	}
	
	private synchronized void processForOneStatus(int postID, Map<Integer, StatusAndListComment> lstInputForSenti){
		
		ListReportData lstRP;
		List<ReportData> listCommentReport;
		lstRP = new ListReportData();
		listCommentReport = new ArrayList<ReportData>();
		
		double sentiComment;
		ReportData commentReport;
		
		for (String comment : lstInputForSenti.get(postID).getListComment()) {
			
			String correctSentence = correctSpellAndEmoticons(comment.toLowerCase());
			correctSentence = correctSentence.replaceAll("[0-9]", REGEX_SPACE);
			String removedURL = replaceURLFromText(correctSentence);
			removedURL = removedURL.replaceAll(
					"[\\<\\>\\|\\/\\:\\#\\)\\(\\%\\+]", "");
			if(removedURL.length() > 7){
				// sentiment value of comment
				sentiComment = runLexiconSentiment(removedURL);
				// create comment report object
				commentReport = new ReportData(
						getTypeOfColor(sentiComment), removedURL);
				listCommentReport.add(commentReport);
			}
		}

		// set list comment
		lstRP.setListCommentData(listCommentReport);
		//return lstRP;
		listPieData.add(lstRP);
	}

	public List<ListReportData> processLexiconSentiment(
			final Map<Integer, StatusAndListComment> lstInputForSenti) {
		
		int numCore = Runtime.getRuntime().availableProcessors();
        final ScheduledExecutorService sES = Executors.newScheduledThreadPool(numCore - 1);
		
		for (final int postID : lstInputForSenti.keySet()) {
			
			Runnable excuteOneStatus = new Runnable() {
                @Override
                public void run() {
                	processForOneStatus(postID, lstInputForSenti);
                }
            };
            sES.execute(excuteOneStatus);
		}
		
		sES.shutdown();
        while (!sES.isTerminated()) {
        }

		return listPieData;
	}
	
	private double runLexiconSentiment(String inputText) {
		
		//System.out.println(inputText);
		double rs = 0.0;
		try {
			String[] rsCheckedAndToken = tokenizer.tokenize(inputText);
			
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
			Map<String, List<String>> lstInputForSenti) {

		List<ListReportData> listPieData = new ArrayList<ListReportData>();

		// loop all of String input
		for (String status : lstInputForSenti.keySet()) {
			double totalScore = 0.0;
			// create pieData stored status
			ListReportData lstRP = new ListReportData();

			// get sentiScore of status
			double sentiStatus = runAnalyzeSentiment(status.toLowerCase());
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
				double sentiComment = runAnalyzeSentiment(comments.toLowerCase());
				// sum of total
//				totalScore += sentiComment;

				// create comment report object
				ReportData commentReport = new ReportData(
						getTypeOfColor(getTypeOfColor(sentiComment)), comments);
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
	
	private double runAnalyzeSentiment(String inputText) {

		double rs = 0.0;
		try {
			// First, we need to correct spelling and covert emoticons
			String correctSentence = correctSpellAndEmoticons(inputText);
			correctSentence = correctSentence.replaceAll("[0-9]", REGEX_SPACE);
			String removeURL = replaceURLFromText(correctSentence);
			// Token each word in this sentence
			String[] rsCheckedAndToken = tokenizer.tokenize(removeURL);
			if (rsCheckedAndToken.length > 0) {
				// Calculate score of this sentence
				rs = ClassifySentiment.getClassifyOfSentiment(rsCheckedAndToken[0]);
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
		} else {
			return NEUTRAL;
		}
	}

	private static String replaceURLFromText(String input) {
		input = input.replaceAll("(https?|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*", " ");
		return input;
	}
	
	private static void writeData(List<ListReportData> reportData) {
		List<String> lstToWrite = new ArrayList<String>();
		Writer writer = null;
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(ExtractOpinion.RESOURCE_PATH
							+ "dataClassify_Draf.csv"), "utf-8"));
			int temp = 0;
			for (ListReportData listReportData : reportData) {
				for (ReportData com : listReportData.getListCommentData()) {
					// count++;
					temp = com.getTypeColor();
					if (temp != 0) {
						if (temp == -1) {
							temp = 0;
						}
						lstToWrite.add(temp + "\t" + com.getContentData());
					}
				}
				for (int i = 0; i < lstToWrite.size(); i++) {
					writer.write(lstToWrite.get(i));
					writer.write("\n");
				}
				lstToWrite.clear();
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			try {
				writer.flush();
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("WRITE DRAF CLASSIFY DATA IS DONE!");
	}

	public static void main(String[] args) {
		SparkUtil.createJavaSparkContext();
		Checker.init();
		VietSentiData.init();
		ClassifySentiment.createClassify();
		
		FBDatabaseProcess fbDB = new FBDatabaseProcess();
		FacebookData fbData = fbDB.getFBDataByPageIDAndDate(new ArrayList<String>(){
			private static final long serialVersionUID = 1L;
			{
				add("541752015846507");//UEL 
				//add("436347149778897");//KTXA
			}
		}, "2015-09-01", "2015-10-15");
		System.out.println("data size: " + fbData.getFbDataForService().size());
		
		SentimentProcessForLexicon sm = new SentimentProcessForLexicon();
		List<ListReportData> result = sm.processLexiconSentiment(fbData.getFbDataForService());
		writeData(result);
	}
}
