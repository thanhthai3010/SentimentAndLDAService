package app.process.sentiment;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import app.process.lda.Stopwords;
import app.utils.spark.SparkUtil;

public class ClassifySentiment implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * dictionay path
	 */
	private final static String DATA_FOR_CLASSIFY = "./src/main/resources/dataClassifyFull.txt";
	
	private final static String CORPUS_PATH = "./src/main/resources/corpus.txt";
	
	private static final Logger logger = LoggerFactory
			.getLogger(ClassifySentiment.class);

	private static HashingTF hashingTF;

	private static IDFModel idfModel;

	private static LogisticRegressionModel logisticRegressionModel;
	
	private static JavaSparkContext sc;
	
	private static List<String> listOfCorpus;

	public ClassifySentiment() {
	}

	public ClassifySentiment(HashingTF hashingTF, IDFModel idfModel,
			LogisticRegressionModel logisticRegressionModel) {
		ClassifySentiment.hashingTF = hashingTF;
		ClassifySentiment.idfModel = idfModel;
		ClassifySentiment.logisticRegressionModel = logisticRegressionModel;
	}
	
	public static void createClassify() {
		
		logger.info("Begin create data for classify");
		
		sc = SparkUtil.getJavaSparkContext();
        // 1.) Load the documents
        JavaRDD<String> dataFull = sc.textFile(DATA_FOR_CLASSIFY).cache();
	    
	    JavaPairRDD<String, Long>  termCounts = dataFull.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterable<String> call(String contents) throws Exception {
				String[] values = contents.split("\t");
				String filter = values[1].replaceAll("[0-9]", "");
				return Arrays.asList(filter.split(" "));
			}
		}).mapToPair(new PairFunction<String, String, Long>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Long> call(String content) throws Exception {
				return new Tuple2<String, Long>(content, 1L);
			}
		}).reduceByKey(new Function2<Long, Long, Long>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Long call(Long count1, Long count2) throws Exception {
				return count1 + count2;
			}
		});
		
		JavaPairRDD<String, Long> afterFilter = termCounts.filter(new Function<Tuple2<String,Long>, Boolean>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Boolean call(Tuple2<String, Long> itemWordCount) throws Exception {
				if (!Stopwords.isStopword(itemWordCount._1)) {
					return true;
				} else {
					return false;
				}
			}
		});
		
		int sizeOfVocabulary = afterFilter.collect().size();
		
		/**
		 * Union positive and negative to get full data
		 */
        // 2.) Hash all documents
        ClassifySentiment.hashingTF = new HashingTF(sizeOfVocabulary);
        JavaRDD<LabeledPoint> tupleData = dataFull.map(content -> {
                String[] datas = content.split("\t");
                String filter = datas[1].replaceAll("[0-9]", "");
                List<String> myList = Arrays.asList(Stopwords.removeStopWords(filter).split(" "));
                return new LabeledPoint(Double.parseDouble(datas[0]), hashingTF.transform(myList));
        }); 
        // 3.) Create a flat RDD with all vectors
        JavaRDD<Vector> hashedData = tupleData.map(label -> label.features());
        // 4.) Create a IDFModel out of our flat vector RDD
        ClassifySentiment.idfModel = new IDF().fit(hashedData);
        // 5.) Create tfidf RDD
        JavaRDD<Vector> idf = idfModel.transform(hashedData);
        
        // 6.) Create Labledpoint RDD
        JavaRDD<LabeledPoint> dataAfterTFIDF = idf.zip(tupleData).map(t -> {
            return new LabeledPoint(t._2.label(), t._1);
        });

	    // random splits data for training and testing
	    JavaRDD<LabeledPoint>[] splits = dataAfterTFIDF.randomSplit(new double[]{0.6, 0.4}, 11L);
	    JavaRDD<LabeledPoint> training = splits[0].cache();
	    
	    // Create a Logistic Regression learner which uses the LBFGS optimizer.
	    LogisticRegressionWithLBFGS lrLearner = new LogisticRegressionWithLBFGS();
	    // Run the actual learning algorithm on the training data.

	    lrLearner.optimizer().setNumIterations(100);
	    ClassifySentiment.logisticRegressionModel = lrLearner.setNumClasses(2).run(training.rdd());
	    
	    JavaRDD<String> corpusSentiment = sc.textFile(CORPUS_PATH).cache();
	    
		/**
		 * Create a list of Vocabulary and set ID increment from 0 for each word.
		 */
	    listOfCorpus = corpusSentiment.collect();
	    
	    logger.info("End create data for classify");
	}

	public HashingTF getHashingTF() {
		return hashingTF;
	}

	public void setHashingTF(HashingTF hashingTF) {
		ClassifySentiment.hashingTF = hashingTF;
	}

	public IDFModel getIdfModel() {
		return idfModel;
	}

	public void setIdfModel(IDFModel idfModel) {
		ClassifySentiment.idfModel = idfModel;
	}

	public LogisticRegressionModel getLogisticRegressionModel() {
		return logisticRegressionModel;
	}

	public void setLogisticRegressionModel(
			LogisticRegressionModel logisticRegressionModel) {
		ClassifySentiment.logisticRegressionModel = logisticRegressionModel;
	}

	public static double getClassifyOfSentiment(String sentiment) {
		double rs = 0.0;
		
		boolean needToClassify = false;
		String removeStopWord = Stopwords.removeStopWords(sentiment);
		
		for (String item : removeStopWord.split(" ")) {
			if (listOfCorpus.contains(item)) {
				needToClassify = true;
				break;
			}
		}
		
		if (needToClassify) {
			// create Vector for this sentiment String
			Vector vectorSentiment = ClassifySentiment.idfModel.transform(ClassifySentiment.hashingTF
					.transform(Arrays.asList(removeStopWord.split(" "))));
			try {
				rs = ClassifySentiment.logisticRegressionModel.predict(vectorSentiment);
				if (rs == 0) {
					rs = -1;
				}
			} catch (Exception e) {
				logger.info("Can not classify this sentiment");
			}
		} else {
			rs = 0;
		}

		return rs;
	}
	public static void main(String[] args) {
		
		SparkUtil.createJavaSparkContext();
		ClassifySentiment.createClassify();
		String input = "Nghe như lớp Pháp_Luật_Đại_Cương của thầy Sang !";
		double rs = ClassifySentiment.getClassifyOfSentiment(input);
		System.out.println(rs);
	}
}
