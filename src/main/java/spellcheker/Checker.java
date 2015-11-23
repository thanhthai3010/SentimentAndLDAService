package spellcheker;

import java.io.Serializable;
import java.util.Map;

/**
 * Check spelling of String[] input
 * 
 * @author thaint
 *
 */
public class Checker implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	static int index = 1;

	/**
	 * default constructor
	 */
	public static void init() {
		// SparkUtil.createJavaSparkContext();
		Dictionary.init();
	}

	/**
	 * get the correct of sentences
	 * 
	 * @param sentences String[]
	 * @return result String[]
	 */
	public static String correct(String sentences) {
		Map<String, String> dictCheckSpell = Dictionary.getDict().collectAsMap();
		
		for (Map.Entry<String, String> entry : dictCheckSpell.entrySet()) {
			sentences = sentences.replaceAll("\\b" + entry.getKey() + "\\b", entry.getValue());
		}
		
		Map<String, String> dictEmoticons = Dictionary.getDictEmoticons().collectAsMap();
		
		for (Map.Entry<String, String> emoticon : dictEmoticons.entrySet()) {
			sentences = sentences.replaceAll("((?<=\\W)|^)\\Q" + emoticon.getKey() + "\\E((?=\\W)|$)", emoticon.getValue());
		}
		
		return sentences;
	}

	/**
	 * get the correct of sentence
	 * 
	 * @param sentence
	 *            String
	 * @return String
	 */
	public static String correctSentence(String sentence) {
		String rplace = sentence.replaceAll("[,.]", "");
		String[] split = rplace.split(" ");
		StringBuilder sb = new StringBuilder();
		for (String inputStr : split) {
			sb.append(correctWord(inputStr));
			sb.append(" ");
		}

		return sb.toString();
	}

	/**
	 * get the correct word
	 * 
	 * @param word
	 *            String
	 * @return word String
	 */
	private static String correctWord(String word) {
		word = Dictionary.getDefination(word);
		return word;
	}
}
