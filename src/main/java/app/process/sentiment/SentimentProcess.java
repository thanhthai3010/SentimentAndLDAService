package app.process.sentiment;

import java.util.ArrayList;
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
			double sentiStatus = runAnalyzeSentiment(item.getStatus());

			// create ReportData stored data for status
			ReportData statusReport = new ReportData(
					getTypeOfColor(sentiStatus), item.getStatus());

			// set status data to List return object
			lstRP.setStatusData(statusReport);

			List<ReportData> listCommentReport = new ArrayList<ReportData>();
			// loop for all comment
			for (String comments : item.getListComment()) {
				// sentiment value of comment
				double sentiComment = runAnalyzeSentiment(comments.toLowerCase());

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
		if (sentiScore > 0.3) {
			return POSITIVE;
		} else if (sentiScore < -0.3) {
			return NEGATIVE;
		} else if (sentiScore >= -0.3 && sentiScore <= 0.3) {
			return NEUTRAL;
		}
		return NEUTRAL;
	}

	private static String replaceURLFromText(String input) {
		input = input.replaceAll("(https?|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*", " ");
		return input;
	}

//	private void writeData(List<String> listComment) {
//
//		List<ReportData> lstPositive = new ArrayList<ReportData>();
//		System.out.println("Lexicon classify comment...");
//		int count = 0;
//		int WriteTimes = 0;
//
//		Writer writerPos = null;
//		try {
//			writerPos = new BufferedWriter(new OutputStreamWriter(
//					new FileOutputStream("DATA_Test.txt"), "utf-8"));
//
//		} catch (UnsupportedEncodingException e1) {
//			e1.printStackTrace();
//		} catch (FileNotFoundException e1) {
//			e1.printStackTrace();
//		}
//
//		int count2 = 0;
//		int count0 = 0;
//		for (String inputText : listComment) {
//
//			inputText = replaceURLFromText(inputText.toLowerCase());
//			// TODO always correctEmoticons before correctSpecialEmoticons
//			inputText = Checker.correctEmoticons(inputText);
//			inputText = Checker.correctSpell(inputText);
//			inputText = Checker.correctSpecialEmoticons(inputText);
//			
//			// TODO
//			inputText = inputText.replace("\\", " ");
//			inputText = inputText.replace("/", " ");
//			inputText = inputText.replace("~", " ");
//			inputText = inputText.replace("`", " ");
//			inputText = inputText.replace("^", " ");
//			inputText = inputText.replace("$", " ");
//			inputText = inputText.replace("!", " ");
//			
//			if (inputText.length() > 4) {
//				String[] rsCheckedAndToken = new String[2];
//				try {
//					rsCheckedAndToken = tokenizer.tokenize(inputText);
//				} catch (Exception e) {
//					System.out.println("can not tokenizer " + inputText);
//				}
//				double sentiScore = runAnalyzeSentiment(rsCheckedAndToken);
//				if (sentiScore >= 0.1) {
//					count2++;
//					if (count2 < 1000) {
//						lstPositive.add(new ReportData(1, rsCheckedAndToken[0]));
//					}
//				} else if (sentiScore <= -0.1) {
//					count0++;
//					if (count0 < 3500) {
//						lstPositive.add(new ReportData(0, rsCheckedAndToken[0]));
//					}
//				}
//
//				count++;
//				if (count > 1000) {
//					count = 0;// reset
//					WriteTimes++;
//					// write
//					System.out.println("Writing data to file for 1000 records in times: "+ WriteTimes);
//					try {
//						for (ReportData item : lstPositive) {
//							writerPos.write(item.getTypeColor() + "\t" + item.getContentData());
//							writerPos.write("\n");
//						}
//					} catch (Exception e) {
//						logger.info(e.getMessage());
//					}
//					
//					lstPositive.clear();
//				}
//			}
//		}
//
//		try {
//			writerPos.flush();
//			writerPos.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	}

	public static void main(String[] args) {
		SparkUtil.createJavaSparkContext();
		Checker.init();
		VietSentiData.init();
		ClassifySentiment.createClassify();

//		Map<String, List<String>> fbDataForSentiment = new LinkedHashMap<String, List<String>>();
//		fbDataForSentiment.put("bực bội quá đi mất", new ArrayList<String>(){/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//		{
//			add(" viết thế này chắc không ai hiểu nhưng vẫn muốn viết cho nhẹ lòng mình. đi hội trại khoa, cho tới lúc kết thúc thì cái người mình để ý mình vẫn chưa làm quen được!! ko chung xe. chập choạng tối thì thấy ấy đứng 1 mình chỗ lối vào trại cả tiếng đồng hồ mà dòm qua dòm lại rất muốn ra bắt chuyện mà ko biết làm sao. lát sau đi với đám bạn (trong đó có bạn của ấy) ra chỗ ấy đứng nói chuyện mới biết là đang canh an ninh (hình như vậy, ko nhớ lắm) gì đấy. xinh xinh mà đứng vậy coi chừng bị bắt mất tích luôn chứ canh gác cái gì. lúc quẩy nhảy rất sung, lúc đi với bạn cũng nhí nhảnh mà sao đứng 1 mình thì như tảng băng thế, không quan tâm cái gì khác. cố tình nhìn chằm chằm mỗi lần chạm mặt chắc cũng chả biết nhỉ. lạnh lùng quá. buồn. lại buồn hơn khi thấy xung quanh ấy có rất nhiều bạn nam, gần như lúc nào nhìn thấy cũng là vậy. bạn nói phải làm sao đây?? mà tóm lại bạn có người yêu chưa? --Cừu--");
//		}});
//		SentimentProcess sm = new SentimentProcess();
//		List<ListReportData> result = sm.processSentiment(fbDataForSentiment);
//		
//		for (ListReportData listReportData : result) {
//			System.out.println("key content " + listReportData.getStatusData().getContentData());
//			System.out.println("key color " + listReportData.getStatusData().getTypeColor());
//			for (ReportData com : listReportData.getListCommentData()) {
//				System.out.println("comment content " + com.getContentData());
//				System.out.println("comment color " + com.getTypeColor());
//			}
//		}
	}
}
