package vietSentiData;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.process.spellcheker.Checker;
import app.utils.dto.ListPieData;
import app.utils.dto.PieChart;
import app.utils.dto.PieData;
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
	public ListPieData processSentiment(List<String> lstInputForSenti) {
		
		ListPieData listPieData = new ListPieData();
		
		// loop all of String input
		for (String inputSenti : lstInputForSenti) {
			double sentiScore = runAnalyzeSentiment(inputSenti);
			int tyleColor = 0;
			if (sentiScore > 0) {
				tyleColor = POSITIVE;
			} else if (sentiScore < 0) {
				tyleColor = NEGATIVE;
			} else if (sentiScore == 0) {
				tyleColor = NEUTRAL;
			}
			PieData pieData = new PieData(tyleColor, inputSenti);
			listPieData.add(pieData);
		}
		
		return listPieData;
	}
	
	/**
	 * Object to draw pie chart
	 * @param lisPieData
	 * @return
	 */
	public List<PieChart> getCharData(ListPieData lisPieData){
		// loop all pieData input to calculate sum of each type color
		
		List<PieChart> lstPieChar = new ArrayList<PieChart>();
		
		int numOfPos = 0;
		int numOfNeg = 0;
		int numOfNeu = 0;
		for (PieData pieData : lisPieData) {
			switch (pieData.getTypeColor()) {
			case POSITIVE:
				numOfPos++;
				break;
			case NEGATIVE:
				numOfNeg++;
				break;
			case NEUTRAL: 
				numOfNeu++;
				break;
			default:
				break;
			}
		}
		
		PieChart piePositive = new PieChart("Positive Percent", numOfPos);
		PieChart pieNegative = new PieChart("Negative Percent", numOfNeg);
		PieChart pieNeutral = new PieChart("Neutral Percent", numOfNeu);
		
		lstPieChar.add(piePositive);
		lstPieChar.add(pieNegative);
		lstPieChar.add(pieNeutral);
		
		return lstPieChar;
	}
	
	public static void main(String[] args) {
		SparkUtil.createJavaSparkContext();
		Checker.init();
		VietSentiData.init();

		SentimentProcess smP = new SentimentProcess();
		List<String> lstInputForSenti = new ArrayList<String>();
		lstInputForSenti
				.add("hai người yêu nhau không gì là không thể. Tin đi bạn");
		lstInputForSenti
				.add("Bỏ đi bạn. Lời khuyên chân thành. Rồi tha thứ làm bạn như bình thường.");
		ListPieData ls = smP.processSentiment(lstInputForSenti);
		for (PieData pieData : ls) {
			System.out.println(pieData.getTypeColor());
			System.out.println(pieData.getContentData());
		}

	}
}
