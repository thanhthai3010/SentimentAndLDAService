package app.process.sentiment;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
		tokenizer = new VietTokenizer();
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
				rs = VietSentiData.scoreTokens(rsCheckedAndToken[0]
						.split(REGEX_SPACE));
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
				rs = VietSentiData.scoreTokens(rsCheckedAndToken[0]
						.split(REGEX_SPACE));
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
				totalScore += sentiComment;

				// create comment report object
				ReportData commentReport = new ReportData(
						getTypeOfColor(getTypeOfColor(sentiComment)), comments);
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
	 * 
	 * @param sentiScore
	 * @return POSITIVE, NEGATIVE or NEUTRAL
	 */
	// TODO separates pie
	private int getTypeOfColor(double sentiScore) {
		if (sentiScore >= 0.02) {
			return POSITIVE;
		} else if (sentiScore <= -0.02) {
			return NEGATIVE;
		} else if (sentiScore > -0.02 && sentiScore < 0.02) {
			return NEUTRAL;
		}
		return NEUTRAL;
	}

	private static String replaceURLFromText(String input) {
		input = input.replaceAll("(https?|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*", " ");
		return input;
	}

	private void writeData(List<String> listComment) {

		List<ReportData> lstPositive = new ArrayList<ReportData>();
//		List<String> lstNegative = new ArrayList<String>();
//		List<String> lstNeutral = new ArrayList<String>();
		System.out.println("Lexicon classify comment...");
		int count = 0;
		int WriteTimes = 0;

		Writer writerPos = null;
//		Writer writerNetr = null;
//		Writer writerNeg = null;
		try {
			writerPos = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("DATA.txt"), "utf-8"));
//			writerNeg = new BufferedWriter(new OutputStreamWriter(
//					new FileOutputStream("NEGATIVE.txt"), "utf-8"));
//			writerNetr = new BufferedWriter(new OutputStreamWriter(
//					new FileOutputStream("NEUTRAL.txt"), "utf-8"));

		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}

		int count2 = 0;
		int count1 = 0;
		int count0 = 0;
		for (String inputText : listComment) {

			inputText = replaceURLFromText(inputText.toLowerCase());
			// TODO always correctEmoticons before correctSpecialEmoticons
			inputText = Checker.correctEmoticons(inputText);
			inputText = Checker.correctSpell(inputText);
			inputText = Checker.correctSpecialEmoticons(inputText);
			
			if (inputText.length() > 4) {
				String[] rsCheckedAndToken = new String[2];
				try {
					rsCheckedAndToken = tokenizer.tokenize(inputText);
				} catch (Exception e) {
					System.out.println("can not tokenizer " + inputText);
				}
				double sentiScore = runAnalyzeSentiment(rsCheckedAndToken);
				if (sentiScore >= 0.1) {
					count2++;
					if (count2 < 1000) {
						lstPositive.add(new ReportData(2, rsCheckedAndToken[0]));
					}
				} else if (sentiScore <= -0.1) {
					count0++;
					if (count0 < 1000) {
						lstPositive.add(new ReportData(0, rsCheckedAndToken[0]));
					}
				} else if (sentiScore > -0.1 && sentiScore < 0.1) {
					count1++;
					if (count1 < 1000) {
						lstPositive.add(new ReportData(1, rsCheckedAndToken[0]));
					}
				}

				count++;
				if (count > 1000) {
					count = 0;// reset
					WriteTimes++;
					// write
					System.out.println("Writing data to file for 1000 records in times: "+ WriteTimes);
					try {
						for (ReportData item : lstPositive) {
							writerPos.write(item.getTypeColor() + "\t" + item.getContentData());
							writerPos.write("\n");
						}
					} catch (Exception e) {
						logger.info(e.getMessage());
					}
					
					lstPositive.clear();
				}
			}
		}

		try {
			writerPos.flush();
			writerPos.close();
		} catch (IOException e) {
			e.printStackTrace();
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
