package app.server.handling;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

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

import app.processing.lda.LDAProcess;
import app.utils.dto.InputDataForLDA;
import app.utils.dto.ListTopic;
import app.utils.dto.TextValue;
import app.utils.dto.Topic;
import scala.Tuple2;
import spellcheker.Checker;
import app.utils.spark.SparkUtil;
import vietSentiData.VietSentiData;
import vn.hus.nlp.tokenizer.VietTokenizer;


public class ServerImpl extends UnicastRemoteObject implements ServerInterf {

	final int MAXTHREAD = 100;
	private static final String IP_THIS_SERVER = "127.0.0.1";

	private static final long serialVersionUID = 1L;
	private Registry rmiRegistry;

	private static VietTokenizer tokenizer;

	public void start() throws Exception {
		rmiRegistry = LocateRegistry.createRegistry(1099);
		rmiRegistry.bind("server", this);
		System.out.println("Server " + IP_THIS_SERVER + " started successful!");
	}

	public void stop() throws Exception {
		rmiRegistry.unbind("server");
		unexportObject(this, true);
		unexportObject(rmiRegistry, true);
		System.out.println("Server stopped");
	}

	public ServerImpl() throws RemoteException, SQLException {
		super();
		try {
			SparkUtil.createJavaSparkContext();
			tokenizer = new VietTokenizer("tokenizer.properties");
			// Check spelling
			Checker.init();
			
			VietSentiData.init();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String hello() throws RemoteException {
		return "Server " + IP_THIS_SERVER + " say: Hi client!";
	}

	@Override
	public double runAnalyzeSentiment(String inputText, boolean isNeedToCheck)
			throws RemoteException {

		if (inputText == null || "".equals(inputText)) {
			return -6.0;
		}

		double rs = -3.0;
		if (isNeedToCheck) {
			// check spell
			try {
				/*String[] rsCheckSpell = check.correct(tokenizer
						.tokenize(inputText));
				rs = VietSentiData.scoreTokens(rsCheckSpell);
				*/
				////////////////////////////////////////////////////
				String[] rsCheckedAndToken = runSpellCheckAndToken(inputText);
				if(rsCheckedAndToken.length > 0){
					rs = VietSentiData.scoreTokens(rsCheckedAndToken);
				}			
				
			} catch (Exception ex) {
				ex.printStackTrace();
				return -3.0;
			}
		} else {
			rs = VietSentiData.scoreTokens(tokenizer.tokenize(inputText));
		}

		return rs;
	}

	@Override
	public String[] runSpellCheckAndToken(String inputText) throws RemoteException {

		// check spell
		//String[] rsCheckSpell = check.correct(tokenizer.tokenize(inputText));
		String temp[] = new String[1];
		temp[0] = inputText.replaceAll("[\n\r]", " ");
//		String[] rsParseEmoticon = check.parseEmoticons(temp);
//		if(rsParseEmoticon != null && rsParseEmoticon.length > 0 && rsParseEmoticon[0] != null){
//			temp = tokenizer.tokenize(rsParseEmoticon[0]);
//		}
		//String[] rsCheckSpell = check.correct(temp);
		String[] rsToken = new String[1];
		//if(rsCheckSpell.length > 0){
		//	rsToken = tokenizer.tokenize(rsCheckSpell[0]);
		//	rs = VietSentiData.scoreTokens(rsToken);
		//}

		return temp;
	}

	// TODO
	@Override
	public void processLDA(InputDataForLDA inputDataForLDA) {
		long startTime = System.currentTimeMillis();
		List<String> beforeSpell = new ArrayList<String>();
		// before processLDA we need to check spelling.
		for (String input : inputDataForLDA.getListOfPostFBForLDA()) {
			String[] tokenText = tokenizer.tokenize(input.replaceAll("[0-9]", ""));			
			String spell = Checker.correct(tokenText[0]);
			beforeSpell.add(spell);
		}
		System.out.println("Finish check spell");
		inputDataForLDA.setListOfPostFBForLDA(beforeSpell);
		LDAProcess.mainProcessLDA(inputDataForLDA);
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Time to run this program: " + TimeUnit.MILLISECONDS.toSeconds(totalTime));
	}

	@Override
	public ListTopic getDescribeTopics() {
		Map<Integer, HashMap<String, Double>> describeTopics = LDAProcess.getDescribeTopics();
		
		ListTopic listTP = new ListTopic();

		for (Integer key : describeTopics.keySet()) {
			Topic tp = new Topic(key);
			HashMap<String, Double> value = describeTopics.get(key);
			for (Entry<String, Double> entry : value.entrySet()) {
				TextValue ns = new TextValue(entry.getKey(), entry.getValue());
				tp.getTextValues().add(ns);
			}
			listTP.add(tp);
		}
		
		return listTP;
	}
	
	
}
