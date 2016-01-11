package app.process.lda;

import java.util.ArrayList;
import java.util.List;
import main.ExtractOpinion;

import org.apache.spark.api.java.JavaSparkContext;

import app.utils.spark.SparkUtil;

public class Stopwords {
	
	/**
	 * JavaSparkContext
	 */
	private static JavaSparkContext sc;
	
	/**
	 * Store a list of StopWords
	 */
	public static List<String> stopWordSet = new ArrayList<String>();
	
	/**
	 * STOPWORDS_PATH file
	 */
	private final static String STOPWORDS_PATH = ExtractOpinion.RESOURCE_PATH + "stopWords.txt";
	
	/**
	 * initial Stop Words
	 */
	public static void init() {
		sc = SparkUtil.getJavaSparkContext();
		stopWordSet = sc.textFile(STOPWORDS_PATH).collect();
	}

	/**
	 * Check String is StopWords or not
	 * @param word String
	 * @return result
	 */
	public static boolean isStopword(String word) {
		if (word.length() < 2)
			return true;
		// if (word.charAt(0) >= '0' && word.charAt(0) <= '9')
		// return true; // remove numbers, "25th", etc
		if (stopWordSet.contains(word.toLowerCase()))
			return true;
		else
			return false;
	}

	/**
	 * Return a String does not containt Stop Words
	 * @param input String
	 * @return String after remove StopWords
	 */
	public static String removeStopWords(String string) {
		String result = "";
		String[] words = string.split("\\s+");
		for (String word : words) {
			if (!word.isEmpty() && !isStopword(word)) {
				result += (word + " ");
			}
		}
		return result;
	}
}
