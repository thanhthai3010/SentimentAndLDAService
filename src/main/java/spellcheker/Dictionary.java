package spellcheker;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import app.utils.spark.SparkUtil;

/**
 * get data for correct dictionary
 * @author thaint
 *
 */
public class Dictionary {
	/**
	 * dictionay path
	 */
	private final static String DICTIONARY_PATH = "./src/main/resources/dictionary.txt";
	
	/**
	 * Facebook emoticons path
	 */
	private final static String EMOTICONS_PATH = "./src/main/resources/emoticon.txt";
	
	/**
	 * store dictionary for check spell
	 */
	private static JavaPairRDD<String, String> dictCheckSpells;
	
	/**
	 * store emoticons for convert to string
	 */
	private static JavaPairRDD<String, String> dictEmoticons;
	
	/**
	 * JavaSparkContext
	 */
	private static JavaSparkContext sc;

	/**
	 * initial data
	 */
	public static void init() {
		sc = SparkUtil.getJavaSparkContext();
		readDictionaryFromFile(DICTIONARY_PATH);
		readEmoticonsFromFile(EMOTICONS_PATH);
	}

	/**
	 * read Dictionary FromFile
	 * @param filePath String
	 */ 
	private static void readDictionaryFromFile(String filePath) {
		JavaRDD<String> dictionaryFile = sc.textFile(filePath);

		dictCheckSpells = dictionaryFile
				.mapToPair(new PairFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(String text)
							throws Exception {
						if(text.split("\t").length > 1){
							return new Tuple2<String, String>(text.split("\t")[0],
								text.split("\t")[1]);
						} else {
							return new Tuple2<String, String>("","");
						}
					}
				});
	}
	
	/**
	 * Read file emoticons
	 * @param filePath
	 */
	private static void readEmoticonsFromFile(String filePath) {
		JavaRDD<String> emoticonsFile = sc.textFile(filePath);

		dictEmoticons = emoticonsFile
				.mapToPair(new PairFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(String text)
							throws Exception {
						if(text.split("\t").length > 1){
							return new Tuple2<String, String>(text.split("\t")[0],
								text.split("\t")[1]);
						} else {
							return new Tuple2<String, String>("","");
						}
					}
				});
	}
	
	/**
	 * Get dictionary for check spell VietNamese
	 * @return JavaPairRDD<String, String> dictionary
	 */
	public static JavaPairRDD<String, String> getDict(){
		return dictCheckSpells;
	}
	
	/**
	 * Get dictionary for check spell VietNamese
	 * @return JavaPairRDD<String, String> dictionary
	 */
	public static JavaPairRDD<String, String> getDictEmoticons(){
		return dictEmoticons;
	}
	
}
