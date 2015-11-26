package app.server.handling;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import app.processing.lda.LDAProcess;
import app.utils.dto.FacebookData;
import app.utils.dto.ListPieData;
import app.utils.dto.ListTopic;
import app.utils.dto.TextValue;
import app.utils.dto.Topic;
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

		String[] rsCheckedAndToken = new String [1];
		String afterSpell = "";
		String[] tokenText = tokenizer.tokenize(inputText.replaceAll("[\n\r0-9]", ""));			
		afterSpell = Checker.correct(tokenText[0]);
		rsCheckedAndToken = afterSpell.split(" ");

		return rsCheckedAndToken;
	}

	// TODO
	@Override
	public void processLDA(FacebookData inputDataForLDA) {
		long startTime = System.currentTimeMillis();
		
		LDAProcess.mainProcessLDA(inputDataForLDA);
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		
		System.out.println("Time to run this program: " + TimeUnit.MILLISECONDS.toSeconds(totalTime));
	}

	@Override
	public ListTopic getDescribleTopics() {
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

	@Override
	public ListPieData processSentiment(int topicID) throws RemoteException {
		
		return null;
	}
	
	
}
