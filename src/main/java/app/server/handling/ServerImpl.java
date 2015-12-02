package app.server.handling;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.processing.database.FBDatabaseProcess;
import app.processing.lda.LDAProcess;
import app.utils.dto.FacebookData;
import app.utils.dto.FacebookDataToInsertDB;
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
	
	private static final Logger logger = LoggerFactory
			.getLogger(ServerImpl.class);
	
	private Registry rmiRegistry;

	
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
			// Check spelling
			Checker.init();
			// Init VietSentidata
			VietSentiData.init();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String hello() throws RemoteException {
		return "Server " + IP_THIS_SERVER + " say: Hi client!";
	}

	// TODO
	@Override
	public void processLDA(FacebookData inputDataForLDA) {
		long startTime = System.currentTimeMillis();

		LDAProcess.mainProcessLDA(inputDataForLDA);
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		
		logger.info("Time to run this program: " + TimeUnit.MILLISECONDS.toSeconds(totalTime));
	}

	/**
	 * get list topic
	 */
	@Override
	public ListTopic getDescribleTopics() {
		Map<Integer, LinkedHashMap<String, Double>> describeTopics = LDAProcess.getDescribeTopics();
		
		ListTopic listTP = new ListTopic();

		for (Integer key : describeTopics.keySet()) {
			Topic tp = new Topic(key);
			LinkedHashMap<String, Double> value = describeTopics.get(key);
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

	@Override
	public void saveFBData(FacebookDataToInsertDB fbDataToInsertDB) {
		FBDatabaseProcess fbDatabase = new FBDatabaseProcess(fbDataToInsertDB);
		// Call save method
		fbDatabase.saveFBData();

	}

	@Override
	public FacebookData getFBDataByPageIDAndDate(List<String> lstPageID,
			String startDate, String endDate) throws RemoteException {
		FBDatabaseProcess fbDatabase = new FBDatabaseProcess();
		return fbDatabase.getFBDataByPageIDAndDate(lstPageID, startDate, endDate);
	}
}
