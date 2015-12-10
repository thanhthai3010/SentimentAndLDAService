package app.process.spellcheker;

import java.io.Serializable;
import java.util.Map;

import com.vdurmont.emoji.EmojiParser;

import vn.hus.nlp.tokenizer.VietTokenizer;
import app.utils.spark.SparkUtil;

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
	
	/**
	 * String blank
	 */
	private static final String STRING_BLANK = "";

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
	public static String correctSpell(String sentences) {
		
		if (sentences != null && !STRING_BLANK.equals(sentences)) {
			Map<String, String> dictCheckSpell = Dictionary.getDict().collectAsMap();
			
			for (Map.Entry<String, String> entry : dictCheckSpell.entrySet()) {
				sentences = sentences.replaceAll("\\b" + entry.getKey() + "\\b", entry.getValue());
			}
		}
		
		return sentences;
	}
	
	/**
	 * get the correct of sentences has emoticons
	 * 
	 * @param String input sentences
	 * @return result String after corrected
	 */
	public static String correctEmoticons(String sentences){
		if (sentences != null && !STRING_BLANK.equals(sentences)) {
			Map<String, String> dictEmoticons = Dictionary.getDictEmoticons().collectAsMap();
			
			for (Map.Entry<String, String> emoticon : dictEmoticons.entrySet()) {
				sentences = sentences.replaceAll("((?<=\\W)|^)\\Q" + emoticon.getKey() + "\\E((?=\\W)|$)", emoticon.getValue());
			}
		}
		return sentences;
	}
	
	/**
	 * get the correct of sentences has emoticons
	 * 
	 * @param String input sentences
	 * @return result String after corrected
	 */
	public static String correctSpecialEmoticons(String sentences){
		if (sentences != null && !STRING_BLANK.equals(sentences)) {
			Map<String, String> dictSpecialEmoticons = Dictionary.getSpecialDictEmoticons().collectAsMap();
			
			// convert sentences input into unicode character
			sentences = EmojiParser.parseToAliases(sentences);
			for (Map.Entry<String, String> emoticon : dictSpecialEmoticons.entrySet()) {
				sentences = sentences.replaceAll(emoticon.getKey(), emoticon.getValue() + " ");
			}
		}
		return sentences;
	}
	
	/**
	 * get the correct of sentences has emoticons
	 * 
	 * @param String input sentences
	 * @return result String after corrected
	 */
	public static String correctUnicodeCharacters(String sentences){
		if (sentences != null && !STRING_BLANK.equals(sentences)) {
			Map<String, String> dictUnicodes = Dictionary.getDictUnicodes().collectAsMap();
			
			for (Map.Entry<String, String> unicode : dictUnicodes.entrySet()) {
				sentences = sentences.replaceAll(unicode.getKey(), unicode.getValue());
				sentences = sentences.replaceAll(unicode.getKey().toUpperCase(), unicode.getValue().toUpperCase());
			}
		}
		return sentences;
	}

	public static void main(String[] args) {
		SparkUtil.createJavaSparkContext();
		Checker.init();
		String outPut = Checker.correctSpecialEmoticons("hôm nay là một ngày 😃");
		VietTokenizer to = new VietTokenizer();
		String[] rs = to.tokenize(outPut);
		for (String string : rs) {
			System.out.println(string);
		}
	}
}
