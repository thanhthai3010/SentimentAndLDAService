package app.utils.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkUtil {

	/**
	 * Create a JavaSparkContext that loads settings from system properties (for
	 * instance, when launching with ./bin/spark-submit).
	 *
	 * @return a JavaSparkContext
	 *
	 */
	private static JavaSparkContext ctx;

	public static void createJavaSparkContext() {
		try {
			SparkConf sparkConf = new SparkConf().setAppName("VietSentiment")
					.setMaster("local");
			ctx = new JavaSparkContext(sparkConf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void stopJavaSparkContext() {
		try {
			ctx.stop();
			ctx.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * get JavaSparkContext from SparkUtils
	 * 
	 * @return ctx JavaSparkContext
	 */
	public static JavaSparkContext getJavaSparkContext() {
		return ctx;
	}

	/**
	 * Create a JavaSparkContext object from a given Spark's master URL
	 *
	 * @param sparkMasterURL
	 *            Spark master URL as "spark://<spark-master-host-name>:7077"
	 * @param applicationName
	 *            application name
	 * @return a JavaSparkContext
	 *
	 */
	public static JavaSparkContext createJavaSparkContext(
			String sparkMasterURL, String applicationName) throws Exception {
		JavaSparkContext ctx = new JavaSparkContext(sparkMasterURL,
				applicationName);
		return ctx;
	}

	/**
	 * Create a JavaSparkContext that loads settings from system properties (for
	 * instance, when launching with ./bin/spark-submit).
	 * 
	 * @param applicationName
	 *            application name
	 *
	 * @return a JavaSparkContext
	 *
	 */
	public static JavaSparkContext createJavaSparkContext(String applicationName)
			throws Exception {
		SparkConf conf = new SparkConf().setAppName(applicationName);
		JavaSparkContext ctx = new JavaSparkContext(conf);
		return ctx;
	}

	public static String version() {
		return "1.5.1";
	}
}
