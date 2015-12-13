package app.process.sentiment;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import vn.hus.nlp.tokenizer.VietTokenizer;
import app.process.spellcheker.Checker;
import app.utils.dto.ReportData;
import app.utils.spark.SparkUtil;

/**
 * get score of String input for check this is positive or negetive
 * 
 * @author thaint
 *
 */
public class VietSentiData implements Serializable {

	private static final int GLOSS_IDX_5 = 5;

	private static final int SYNSET_TERMS_IDX_4 = 4;

	private static final int NEG_SCORE_IDX_3 = 3;

	private static final int POS_SCORE_IDX_2 = 2;

	private static final int ID_IDX_1 = 1;

	private static final int POS_IDX_0 = 0;
	
	private static List<String> lstCorpus = new ArrayList<String>();

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * JavaSparkContext input
	 */
	private static JavaSparkContext sc;

	/**
	 * path to VietSentiWordNet file
	 */
	private static String PATH_TO_VSWN = "./src/main/resources/VietSentiWordNetopen1.1.txt";

	/**
	 * path to negative_words file
	 */
	private static String PATH_TO_NEGATIVE_WORD = "./src/main/resources/negative.txt";

	/**
	 * store value dictionary of VSWN
	 */
	private static HashMap<String, Double> dictVSWN = new HashMap<String, Double>();

	/**
	 * store value dictionary of negative_words
	 */
	private static List<String> dictNegative = new ArrayList<String>();

	// From String to list of doubles.
	static HashMap<String, HashMap<Integer, Double>> tempDictionary = new HashMap<String, HashMap<Integer, Double>>();

	/**
	 * initial data
	 */
	public static void init() {
		sc = SparkUtil.getJavaSparkContext();
		readSentiData();
		readNegativeWord();
	}

	/**
	 * read data from VSWN file
	 */
	public static void readSentiData() {
		JavaRDD<String> vSMN = sc.textFile(PATH_TO_VSWN);

		// map data input into RowData
		JavaRDD<RowData> rdd_VSMN = vSMN.map(new Function<String, RowData>() {
			private static final long serialVersionUID = 1L;

			public RowData call(String line) throws Exception {
				String[] fields = line.split("\t");

				RowData rowDT = new RowData(fields[POS_IDX_0],
						fields[ID_IDX_1], Double
								.valueOf(fields[POS_SCORE_IDX_2]), Double
								.valueOf(fields[NEG_SCORE_IDX_3]),
						fields[SYNSET_TERMS_IDX_4], fields[GLOSS_IDX_5]);
				
				return rowDT;
			}
		});

		// loop all record to create dictionary
		rdd_VSMN.foreach(new VoidFunction<RowData>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(RowData rowdt) throws Exception {

				String wordTypeMarker = rowdt.getPos();
				// Calculate synset score as score = PosS - NegS
				Double synsetScore = rowdt.getPosScore() - rowdt.getNegScore();
				// Get all Synset terms
				String[] synTermsSplit = rowdt.getSynsetTerms().split(" ");

				String prev = "";
				// Go through all terms of current synset.
				for (String synTermSplit : synTermsSplit) {
					// Get synterm and synterm rank

					if (synTermSplit.contains("#")) {
						String[] synTermAndRank = (prev + synTermSplit)
								.split("#");
						String synTerm = synTermAndRank[0] + "#"
								+ wordTypeMarker;

						prev = "";
						int synTermRank = 0;
						try {

							synTermRank = Integer.parseInt(synTermAndRank[1]);
						} catch (Exception e) {
							// TODO: handle exception
							System.out.println("sdasd");
						}
						// What we get here is a map of the type:
						// term -> {score of synset#1, score of synset#2...}

						// Add map to term if it doesn't have one
						if (!tempDictionary.containsKey(synTerm)) {
							tempDictionary.put(synTerm,
									new HashMap<Integer, Double>());
						}

						// Add synset link to synterm
						tempDictionary.get(synTerm).put(synTermRank,
								synsetScore);
					} else {
						prev = prev + synTermSplit + "_";
					}
				}

			}
		});

		// Go through all the terms.
		for (Map.Entry<String, HashMap<Integer, Double>> entry : tempDictionary
				.entrySet()) {
			String word = entry.getKey();
			Map<Integer, Double> synSetScoreMap = entry.getValue();

			// Calculate weighted average. Weigh the synsets according to
			// their rank.
			// Score= 1/2*first + 1/3*second + 1/4*third ..... etc.
			// Sum = 1/1 + 1/2 + 1/3 ...
			double score = 0.0;
			double sum = 0.0;
			for (Map.Entry<Integer, Double> setScore : synSetScoreMap
					.entrySet()) {
				score += setScore.getValue() / (double) setScore.getKey();
				sum += 1.0 / (double) setScore.getKey();
			}
			score /= sum;

			dictVSWN.put(word, score);
			
			//TODO
			lstCorpus.add(word);
		}

	}

	/**
	 * Get total score of String word input
	 * 
	 * @param word
	 *            String
	 * @return total_score double
	 */
	public static double extract(String word) {
		double total = 0.0;
		if (dictVSWN.get(word + "#n") != null)
			total = dictVSWN.get(word + "#n") + total;
		if (dictVSWN.get(word + "#a") != null)
			total = dictVSWN.get(word + "#a") + total;
		if (dictVSWN.get(word + "#r") != null)
			total = dictVSWN.get(word + "#r") + total;
		if (dictVSWN.get(word + "#v") != null)
			total = dictVSWN.get(word + "#v") + total;
		return total;
	}

	/**
	 * Get score of each element in String input
	 * 
	 * @param words
	 *            String[]
	 * @return score double
	 */
	public static double scoreTokens(String[] words) {

		int numOfPos = 0;
		int numOfNeg = 0;
		double posScore = 0.0;
		double negScore = 0.0;

		double totalScore = 0.0;
		// qtran
		// flag to check if the previous is a negative word
		boolean isNegativeBefore = false;
		for (String word : words) {
			
			double senti = extract(word);

			if (dictNegative.contains(word)) {
				isNegativeBefore = true;
				continue;
			}

			if (isNegativeBefore) {
				senti = (senti * -1);
				isNegativeBefore = false;
			} else {
				isNegativeBefore = false;
			}
			// increment
			if (senti > 0) {
				posScore += senti;
				numOfPos++;
			} else if (senti < 0) {
				negScore += senti;
				numOfNeg++;
			}
		}

		// convert to 1
		if (numOfPos == 0) {
			numOfPos = 1;
		}
		if (numOfNeg == 0) {
			numOfNeg = 1;
		}
		
		totalScore = (posScore / numOfPos) + (negScore / numOfNeg);
		

		return totalScore;
	}

	/**
	 * read data from negative_words file
	 */
	public static void readNegativeWord() {
		JavaRDD<String> negativeWord = sc.textFile(PATH_TO_NEGATIVE_WORD);
		dictNegative = negativeWord.collect();
	}
	
	public static void main(String[] args) {
		
		SparkUtil.createJavaSparkContext();
		Checker.init();
		VietSentiData.init();
		
		VietTokenizer tk = new VietTokenizer();
		String ip = "Mọi người ơi cho mình ý kiến đi ạ. Mình là nam năm 1 học KHTN và mình đã lỡ thích 1 chị bên IU. Chị ấy hơn mình tận 2 tuổi. "
				+ "Trước giờ mình chưa từng thích người lớn tuổi hơn mình nhưng giờ lỡ rồi, mình không biết làm sao hết. "
				+ "Chị ấy đẹp lắm nhìn thích lắm. Nhưng hình như chị chưa thích mình. Giờ mình phải làm sao ạ. "
				+ "Mọi người giúp mình với. :(";
		String[] rs = tk.tokenize(Checker.correctEmoticons(ip));
		
		double score = VietSentiData.scoreTokens(rs[0].split(" "));
		System.out.println("Score " + score);
		
		Writer writerCorpus = null;
		try {
			writerCorpus = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream("corpus.txt"), "utf-8"));

		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		
		try {
			for (String item : VietSentiData.lstCorpus) {
				writerCorpus.write(item + "\n");
			}
		} catch (Exception e) {
		}
		
		System.out.println("done save");
	}
}
