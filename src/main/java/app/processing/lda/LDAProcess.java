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
import java.util.HashMap;
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

import scala.Tuple2;
import spellcheker.Checker;
import vn.hus.nlp.tokenizer.VietTokenizer;
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
	 * Determine number of Documents are talking about each Topic
	 */
	private static final int MAX_DOCUMENT_PER_TOPIC_20 = 20;

	/**
	 * Max words for each topic is there
	 */
	private static final int MAX_TERMS_PER_TOPIC_10 = 10;

	/**
	 * Number of iterations
	 */
	private static final int MAX_ITERATIONS_10 = 10;
	
	/**
	 * Some parameter to run LDA Model
	 * TODO: need to estimate value of this k-means
	 */
	private static final int DEFAULT_NUMBER_OF_TOPIC = 2;
	
	/**
	 * This will contain
	 * TopicID, <Word, Probability>
	 */
	private static Map<Integer, HashMap<String, Double>> describeTopic = new HashMap<Integer, HashMap<String, Double>>();

	/**
	 * This will contain
	 * TopicID: <DocumentID, Probability>
	 */
	private static Map<Integer, HashMap<Long, Double>> topicDoc = new HashMap<Integer, HashMap<Long,Double>>();
	
	private static VietTokenizer tokenizer;
	
	private static FacebookData fbDataForSentiment;
	
	/**
	 * JavaSparkContext
	 */
	private static JavaSparkContext sc;

	public static void mainProcessLDA(FacebookData inputDataForLDA) {
		
		fbDataForSentiment = inputDataForLDA;
	
		tokenizer = new VietTokenizer("tokenizer.properties");
		
		List<String> checkSpell = new ArrayList<String>();
		
		// before processLDA we need to check spelling.
		for (String input : inputDataForLDA.getFbDataForService().keySet()) {
			
			String[] tokenText = tokenizer.tokenize(input.replaceAll("[0-9]", ""));			
			String spell = Checker.correct(tokenText[0]);
			checkSpell.add(spell);
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
						return new Tuple2<String, Long>(s.toLowerCase(), 1L);
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

		int sizeOfVocabulary = termCounts.size();
		List<String> vocabulary = new ArrayList<String>();
		for (int i = 0; i < sizeOfVocabulary; i++) {
			vocabulary.add(termCounts.get(i)._1);
		}

		final HashMap<String, Long> vocabAndCount = new HashMap<String, Long>();
		for (Tuple2<String, Long> item : sc.parallelize(vocabulary)
				.zipWithIndex().collect()) {
			vocabAndCount.put(item._1, item._2);
		}

		/**
		 * Create input data for LDA Model
		 * Create a vector Word-Count vector
		 */
		JavaRDD<Tuple2<Long, Vector>> documents = wordCountVector(corpuss, vocabAndCount);

		/**
		 * Convert from JavaRDD to JavaPairRDD
		 */
		JavaPairRDD<Long, Vector> inputVectorForLDA = JavaPairRDD.fromJavaRDD(documents);

		/**
		 * Run LDA model
		 */
		DistributedLDAModel ldaModel = runLDAModel(inputVectorForLDA, MAX_ITERATIONS_10);

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
		Tuple2<long[], double[]>[] topicDocuments = ldaModel.topDocumentsPerTopic(MAX_DOCUMENT_PER_TOPIC_20);
		
		int idxTopicID = 1;
		for (Tuple2<long[], double[]> tpDoc : topicDocuments) {
			HashMap<Long, Double> valueOfRs = new HashMap<Long, Double>();
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
				HashMap<Long, Double> value = topicDoc.get(key);
				for (Entry<Long, Double> entry : value.entrySet()) {
					writer.write(entry.getKey() + ":\t" + entry.getValue()
							+ "\n");
				}

			}
			writer.write("\n");
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		int idxOfTopic = 1;
		for (Tuple2<int[], double[]> topic : topicIndices) {
			HashMap<String, Double> valueOfRs = new HashMap<String, Double>();
			
			int[] terms = topic._1;
			double[] termWeights = topic._2;
			
			for (int i = 0; i < terms.length; i++) {
				valueOfRs.put(vocabulary.get(terms[i]), termWeights[i]);
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
				HashMap<String, Double> value = describeTopic.get(key);
				for (Entry<String, Double> entry : value.entrySet()) {
					writer.write(entry.getKey() + ":\t" + entry.getValue() + "\n");
				}
			}
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		
		//THAINT
		getFbDataForSentiment(1);
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
		
		return result;
		
	}
	
	/**
	 * word count vectors (columns: terms [vocabulary], rows [documents])
	 * @param corpuss
	 * @param vocabAndCount
	 * @return
	 */
	public static JavaRDD<Tuple2<Long, Vector>> wordCountVector(JavaRDD<List<String>> corpuss, HashMap<String, Long> vocabAndCount) {
		JavaRDD<Tuple2<Long, Vector>> result = corpuss
		.zipWithIndex()
		.map(new Function<Tuple2<List<String>, Long>, Tuple2<Long, Vector>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Vector> call(Tuple2<List<String>, Long> t) throws Exception {
				HashMap<Long, Double> counts = new HashMap<Long, Double>(0);
				for (String item : t._1) {
					if (vocabAndCount.containsKey(item)) {
						Long idx = vocabAndCount.get(item);
						if (!counts.containsKey(idx)) {
							counts.put(idx, 0.0);
						}
						counts.put(idx, counts.get(idx) + 1.0);
					}
				}
				int[] key = new int[counts.size()];
				double[] value = new double[counts.size()];

				int i = 0;
				for (Long item : counts.keySet()) {
					key[i] = item.intValue();
					value[i] = counts.get(item);
					i++;

				}
				return new Tuple2<Long, Vector>(t._2, Vectors.sparse(
						vocabAndCount.size(), key, value));
			}
		});
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
		
		for (int i = DEFAULT_NUMBER_OF_TOPIC; i < 8; i++) {
			
			DistributedLDAModel estimateLDA =	(DistributedLDAModel) new LDA()
			.setK(i).setMaxIterations(maxIterations).run(inputVectorForLDA);
			
			// get logLikelihood() value:
			double logLikelihood = estimateLDA.logLikelihood();
			arrLog.add(logLikelihood);
		}
		Double maxValue = Collections.max(arrLog);
		int theBestNumberOfTopic = arrLog.indexOf(maxValue) + DEFAULT_NUMBER_OF_TOPIC;
		
		ldaModel =	(DistributedLDAModel) new LDA()
		.setK(theBestNumberOfTopic).setMaxIterations(maxIterations).run(inputVectorForLDA);
		
		System.out.println("Done");
		return ldaModel;
	}
	
	/**
	 * Get HashMap of Topic to create word-cloud
	 * @return Map<Integer, HashMap<String, Double>>
	 */
	public static Map<Integer, HashMap<String, Double>> getDescribeTopics(){
		return describeTopic;
	}
	
	/**
	 * Get HashMap using for calculate sentiment score
	 * @return Map<Integer, HashMap<Long, Double>>
	 */
	public static Map<Integer, HashMap<Long, Double>> getTopicDocs() {
		return topicDoc;
	}
	
	public static List<String> getFbDataForSentiment(int topicID){
		
		List<String> commentsForSentiment = new ArrayList<String>();
		
		HashMap<Long, Double> listDocuments = topicDoc.get(topicID);
		List<Long> listDocumentID = new ArrayList<Long>(listDocuments.keySet());
		
		// get list of status and get list of comment
		// and get list of
		List<String> listStatus = new ArrayList<String>(fbDataForSentiment.getFbDataForService().keySet());
		
		List<String> commentTemp = new ArrayList<String>();
		for (Long dcID : listDocumentID) {
			commentTemp.add(listStatus.get(Integer.parseInt(dcID.toString())));
		}
		
		//
		for (String item : commentTemp) {
			commentsForSentiment.add(item);
		}
		
		for (String it : commentTemp) {
			for (String comment : fbDataForSentiment.getFbDataForService().get(it)) {
				commentsForSentiment.add(comment);
			}
		}
		
		return commentsForSentiment;
		
	}
	
}
