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
		Dictionary.init();
	}

	/**
	 * get the correct of sentences
	 * 
	 * @param String input sentences
	 * @return result String after corrected
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

}
