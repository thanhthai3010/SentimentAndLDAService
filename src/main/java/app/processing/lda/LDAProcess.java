package app.processing.lda;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import vn.hus.nlp.tokenizer.VietTokenizer;
import app.process.spellcheker.Checker;
import app.utils.dto.FacebookData;
import app.utils.spark.SparkUtil;

/**
 * Implement Process of LDA Model
 * @author thaint
 *
 */
public class LDAProcess implements Serializable {


	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * String blank
	 */
	private static final String STRING_BLANK = "";
	
	private static final Logger logger = LoggerFactory
			.getLogger(LDAProcess.class);
	
	/**
	 * The offset value for documents per topic
	 */
	private static final int OFFSET_DOCUMENTS_PER_TOPIC = 3;

	/**
	 * Max words for each topic is there
	 */
	private static final int MAX_TERMS_PER_TOPIC_10 = 10;

	/**
	 * Number of iterations
	 */
	private static final int MAX_ITERATIONS = 20;
	
	/**
	 * Some parameter to run LDA Model
	 * TODO: need to estimate value of this k-means
	 */
	private static final int DEFAULT_NUMBER_OF_TOPIC = 2;
	
	/**
	 * Max value of number of topics
	 */
	private static final int MAX_NUMBER_TOPIC = 10;
	
	/**
	 * This will contain
	 * TopicID, <Word, Probability>
	 */
	private static Map<Integer, LinkedHashMap<String, Double>> describeTopic = new LinkedHashMap<Integer, LinkedHashMap<String, Double>>();

	/**
	 * This will contain
	 * TopicID: <DocumentID, Probability>
	 */
	private static Map<Integer, LinkedHashMap<Long, Double>> topicDoc = new LinkedHashMap<Integer, LinkedHashMap<Long,Double>>();
	
	/**
	 * VietTokenizer
	 */
	private static VietTokenizer tokenizer;
	
	/**
	 * This variable store data for Sentiment Processing.
	 */
	private static FacebookData fbDataForSentiment;
	
	/**
	 * Determine number of topic, after calculate using logLikelihood function
	 */
	private static int theBestNumberOfTopic = 1;
	
	/**
	 * JavaSparkContext
	 */
	private static JavaSparkContext sc;

	/**
	 * Function to process LDA, this method using FacebookData from client
	 * @param inputDataForLDA FacebookData
	 */
	public static void mainProcessLDA(FacebookData inputDataForLDA) {
		
		/**
		 * Stored data for sentiment process
		 */
		fbDataForSentiment = inputDataForLDA;
	
		tokenizer = new VietTokenizer("tokenizer.properties");
		
		List<String> checkSpell = new ArrayList<String>();
		
		// before processLDA we need to check spelling.
		for (String input : inputDataForLDA.getFbDataForService().keySet()) {
			
			// Check spell before tokenizer
			String spell = Checker.correctSpell(input);
			String[] tokenText = tokenizer.tokenize(spell.replaceAll("[0-9]", STRING_BLANK));			
			
			checkSpell.add(tokenText[0]);
		}
		
		// get JavaSparkContext from SparkUtil
		sc = SparkUtil.getJavaSparkContext();

		JavaRDD<String> data = sc.parallelize(checkSpell);

		/**
		 * Transform data input into a List of VietNamese Words
		 */
		JavaRDD<List<String>> corpus = transformInputData(data);

		//corpus.cache();

		List<List<String>> afterFilterStopword = new ArrayList<List<String>>();
		
		/**
		 * In this step, we will remove all of StopWords
		 */
		afterFilterStopword = filterOutStopWord(corpus);


		JavaRDD<List<String>> corpuss = sc.parallelize(afterFilterStopword);
		
		/**
		 * termCounts using word-count
		 */
		List<Tuple2<String, Long>> termCounts = corpuss
				.flatMap(new FlatMapFunction<List<String>, String>() {

					private static final long serialVersionUID = 1L;

					public Iterable<String> call(List<String> list) {
						return list;
					}
				}).mapToPair(new PairFunction<String, String, Long>() {
					private static final long serialVersionUID = 1L;

					public Tuple2<String, Long> call(String s) {
						// Check if all of s is UPPER CASE
						String result = STRING_BLANK;
						if (s.equals(s.toUpperCase())) {
							result = s;
						} else {
							result = s.toLowerCase();
						}
						return new Tuple2<String, Long>(result, 1L);
					}
				}).reduceByKey(new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					public Long call(Long i1, Long i2) {
						return i1 + i2;
					}
				}).collect();

		/**
		 * Sort each term by count
		 */
		Collections.sort(termCounts, new Comparator<Tuple2<String, Long>>() {
			@Override
			public int compare(Tuple2<String, Long> v1, Tuple2<String, Long> v2) {
				return (int) (v2._2 - v1._2);
			}
		});

		/**
		 * Create a list of Vocabulary
		 */
		int sizeOfVocabulary = termCounts.size();
		List<String> vocabularys = new ArrayList<String>();
		for (int i = 0; i < sizeOfVocabulary; i++) {
			vocabularys.add(termCounts.get(i)._1);
		}

		/**
		 * Create a list of Vocabulary and set ID increment from 0 for each word.
		 */
		final Map<String, Long> wordAndIndexOfWord = new LinkedHashMap<String, Long>();
		for (Tuple2<String, Long> item : sc.parallelize(vocabularys)
				.zipWithIndex().collect()) {
			wordAndIndexOfWord.put(item._1, item._2);
		}

		/**
		 * Create input data for LDA Model
		 * Create a vector Word-Count vector
		 */
		JavaRDD<Tuple2<Long, Vector>> documents = wordCountVector(corpuss, wordAndIndexOfWord);

		/**
		 * Convert from JavaRDD to JavaPairRDD
		 */
		JavaPairRDD<Long, Vector> inputVectorForLDA = JavaPairRDD.fromJavaRDD(documents);

		/**
		 * Run LDA model
		 */
		DistributedLDAModel ldaModel = runLDAModel(inputVectorForLDA, MAX_ITERATIONS);

//		JavaRDD<Tuple2<Object, Vector>> topicdistributes = ldaModel
//				.topicDistributions().toJavaRDD();
		/**
		 * Get describe for each Topics
		 * In this case, this is: Topic: term1, term2
		 */
		Tuple2<int[], double[]>[] topicIndices = ldaModel.describeTopics(MAX_TERMS_PER_TOPIC_10);

		/**
		 * We need to know how many Documents are talking about each Topic
		 */
		// TODO: how to know how many document are talking about each topic.
		
		int documentPerTopic = (int)(inputVectorForLDA.count() / theBestNumberOfTopic) + OFFSET_DOCUMENTS_PER_TOPIC;
		
		Tuple2<long[], double[]>[] topicDocuments = ldaModel.topDocumentsPerTopic(documentPerTopic);
		
		int idxTopicID = 1;
		for (Tuple2<long[], double[]> tpDoc : topicDocuments) {
			LinkedHashMap<Long, Double> valueOfRs = new LinkedHashMap<Long, Double>();
			for (int i = 0; i < tpDoc._1.length; i++) {
				valueOfRs.put(tpDoc._1[i], tpDoc._2[i]);
			}
			topicDoc.put(idxTopicID, valueOfRs);
			// increment value index of TopicID
			idxTopicID++;

		}
		
		/**
		 * writing data Topic- List of Documents
		 */
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("TOPIC-DOCUMENT.txt"), "utf-8"))) {

			for (int key : topicDoc.keySet()) {
				writer.write("TOPIC: ");

				// write key
				writer.write(key + "\n");
				LinkedHashMap<Long, Double> value = topicDoc.get(key);
				for (Entry<Long, Double> entry : value.entrySet()) {
					writer.write(entry.getKey() + ":\t" + entry.getValue()
							+ "\n");
				}

			}
			writer.write("\n");
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		
		int idxOfTopic = 1;
		for (Tuple2<int[], double[]> topic : topicIndices) {
			LinkedHashMap<String, Double> valueOfRs = new LinkedHashMap<String, Double>();
			
			int[] terms = topic._1;
			double[] termWeights = topic._2;
			
			for (int i = 0; i < terms.length; i++) {
				valueOfRs.put(vocabularys.get(terms[i]), termWeights[i]);
			}
			describeTopic.put(idxOfTopic, valueOfRs);
			// increment value index of TopicID
			idxOfTopic++;

		}
		
		/**
		 *  Write describe of Topics, in this case is terms(words) and it's probability
		 */
		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream("DescribeOfTopics.txt"), "utf-8"))) {
			
			
			for (int key : describeTopic.keySet()) {
				writer.write("TOPIC: " + key + "\n");

				// write key
				LinkedHashMap<String, Double> value = describeTopic.get(key);
				for (Entry<String, Double> entry : value.entrySet()) {
					writer.write(entry.getKey() + ":\t" + entry.getValue() + "\n");
				}
			}
			
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		
		logger.info("Done LDA processing");
	}

	/**
	 * Split each sentences input into a List of VietNamese Words
	 * @param data JavaRDD<String>
	 * @return List of each words
	 */
	public static JavaRDD<List<String>> transformInputData(JavaRDD<String> data) {
		/**
		 * Split each sentences input into a List of VietNamese Words
		 */
		JavaRDD<List<String>> corpus = data.map(new Function<String,  List<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public List<String> call(String content) throws Exception {
				String[] string_arrays = content.split("\\s");
				return Arrays.asList(string_arrays);
			}
		});
		logger.info("Done transform data");
		return corpus;
	}
	
	/**
	 * Remove StopWord
	 * @param inputSentences
	 * @return
	 */
	public static List<List<String>> filterOutStopWord(JavaRDD<List<String>> inputSentences){
		
		List<List<String>> result = new ArrayList<List<String>>();
		
		for (List<String> item : inputSentences.collect()) {
			List<String> tmp = sc.parallelize(item)
					.filter(new Function<String, Boolean>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(String s) throws Exception {
							return s.length() > 3 && !Stopwords.isStopword(s);
						}
					}).collect();
			result.add(tmp);
		}
		logger.info("Done filter stopWord");
		return result;
		
	}
	
	/**
	 * word count vectors (columns: terms [vocabulary], rows [documents])
	 * @param corpuss
	 * @param vocabAndCount
	 * @return
	 */
	public static JavaRDD<Tuple2<Long, Vector>> wordCountVector(JavaRDD<List<String>> corpuss, Map<String, Long> wordAndIndexOfWord) {
		JavaRDD<Tuple2<Long, Vector>> result = corpuss
		.zipWithIndex()
		.map(new Function<Tuple2<List<String>, Long>, Tuple2<Long, Vector>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Vector> call(Tuple2<List<String>, Long> listWord) throws Exception {
				Map<Long, Double> wordIDAndWordCount = new LinkedHashMap<Long, Double>(0);
				for (String item : listWord._1) {
					// For each word in listWord of Document input
					if (wordAndIndexOfWord.containsKey(item.toLowerCase())) {
						// Check if vocabAndCount has contain this word
						
						// Get Id of Word.
						Long idxOfWord = wordAndIndexOfWord.get(item.toLowerCase());
						if (!wordIDAndWordCount.containsKey(idxOfWord)) {
							// if this is the first time. Add to HashMap value <idOfWord, 0>
							wordIDAndWordCount.put(idxOfWord, 0.0);
						}
						// Increase the number of appear
						wordIDAndWordCount.put(idxOfWord, wordIDAndWordCount.get(idxOfWord) + 1.0);
					}
				}
				/**
				 * create array stored posisions
				 */
				int[] key = new int[wordIDAndWordCount.size()];
				
				/**
				 * value at each posision
				 */
				double[] value = new double[wordIDAndWordCount.size()];

				int i = 0;
				for (Long wordIndex : wordIDAndWordCount.keySet()) {
					// get wordIndex
					key[i] = wordIndex.intValue();
					// wordCount of wordIndex
					value[i] = wordIDAndWordCount.get(wordIndex);
					i++;

				}
				
				/**
				 * Create vector base: using parameter
				 * listWord._2: Id of document input
				 * wordAndWordID.size(): size of Vector, it means we have vector with size column
				 * key: the index of column
				 * value: the value in key posision
				 */
				return new Tuple2<Long, Vector>(listWord._2, Vectors.sparse(
						wordAndIndexOfWord.size(), key, value));
			}
		});
		logger.info("Done word-count vector");
		
		return result;
	}

	/**
	 * Run LDA Model with estimate number of Topic
	 * @param inputVectorForLDA a Vecotor For LDA Model
	 * @param maxIterations number of iterations
	 * @return DistributedLDAModel
	 */
	private static DistributedLDAModel runLDAModel(JavaPairRDD<Long, Vector> inputVectorForLDA, int maxIterations){
		DistributedLDAModel ldaModel = null;
		
		List<Double> arrLog = new ArrayList<Double>();
		
		for (int i = DEFAULT_NUMBER_OF_TOPIC; i < MAX_NUMBER_TOPIC; i++) {
			
			DistributedLDAModel estimateLDA =	(DistributedLDAModel) new LDA()
			.setK(i).setMaxIterations(maxIterations).run(inputVectorForLDA);
			
			// get logLikelihood() value:
			double logLikelihood = estimateLDA.logLikelihood();
			arrLog.add(logLikelihood);
		}
		Double maxValue = Collections.max(arrLog);
		theBestNumberOfTopic = arrLog.indexOf(maxValue) + DEFAULT_NUMBER_OF_TOPIC;
		
		ldaModel =	(DistributedLDAModel) new LDA()
		.setK(theBestNumberOfTopic).setMaxIterations(maxIterations).run(inputVectorForLDA);
		
		logger.info("Done runLDAModel");
		return ldaModel;
	}
	
	/**
	 * Get HashMap of Topic to create word-cloud
	 * @return Map<Integer, HashMap<String, Double>>
	 */
	public static Map<Integer, LinkedHashMap<String, Double>> getDescribeTopics(){
		return describeTopic;
	}
	
	/**
	 * Get HashMap using for calculate sentiment score
	 * @return Map<Integer, HashMap<Long, Double>>
	 */
	public static Map<Integer, LinkedHashMap<Long, Double>> getTopicDocs() {
		return topicDoc;
	}
	
	/**
	 * Get data for Sentimen Process
	 * @param topicID value of topicID, user has clicked
	 * @return List<String> containt status and comment of topicID
	 */
	public static Map<String, List<String>> getFbDataForSentiment(int topicID){
		
		/**
		 * This variable will store data for Sentiment Process
		 */
		Map<String, List<String>> sttAndCm = new LinkedHashMap<String, List<String>>();
		
		/**
		 * get list documents are talking about topicID
		 */
		List<Long> listDocumentID = new ArrayList<Long>(topicDoc.get(topicID).keySet());
		
		/**
		 * Get list all of status
		 */
		List<String> listAllOfStatus = new ArrayList<String>(fbDataForSentiment.getFbDataForService().keySet());
		
		/**
		 * List status with documentID
		 */
		List<String> listStatusWithDocument = new ArrayList<String>();
		
		/**
		 * Get all of status of specify documentID
		 */
		for (Long dcID : listDocumentID) {
			listStatusWithDocument.add(listAllOfStatus.get(Integer.parseInt(dcID.toString())));
		}
		
		/**
		 * In this step, we will get specify comment of each documentID
		 */
		for (String status : listAllOfStatus) {
			List<String> lstComment = fbDataForSentiment.getFbDataForService().get(status);
			sttAndCm.put(status, lstComment);
		}
		
		return sttAndCm;
		
	}
	
}
