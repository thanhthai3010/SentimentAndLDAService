package app.server.handling;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.process.sentiment.ClassifySentiment;
import app.process.sentiment.SentimentProcess;
import app.process.sentiment.VietSentiData;
import app.process.spellcheker.Checker;
import app.process.database.FBDatabaseProcess;
import app.process.lda.LDAProcess;
import app.utils.dto.FacebookData;
import app.utils.dto.FacebookDataToInsertDB;
import app.utils.dto.ListReportData;
import app.utils.dto.ListTopic;
import app.utils.dto.Page_Info;
import app.utils.dto.StatusAndListComment;
import app.utils.dto.TextValueWordCloud;
import app.utils.dto.Topic;
import app.utils.spark.SparkUtil;


public class ServerImpl extends UnicastRemoteObject implements ServerInterf {
	
	final int MAXTHREAD = 100;
	
	private static final String IP_THIS_SERVER = "127.0.0.1";

	private static final long serialVersionUID = 1L;
	
	private static final Logger logger = LoggerFactory
			.getLogger(ServerImpl.class);
	
	private Registry rmiRegistry;

	/**
	 * facebook database process
	 */
	private FBDatabaseProcess fbDatabase;
	
	/**
	 * Start service RMI
	 * @throws Exception
	 */
	public void start() throws Exception {
		rmiRegistry = LocateRegistry.createRegistry(1099);
		rmiRegistry.bind("server", this);
		System.out.println("Server " + IP_THIS_SERVER + " started successful!");
	}

	/**
	 * Stop service RMI
	 * @throws Exception
	 */
	public void stop() throws Exception {
		rmiRegistry.unbind("server");
		unexportObject(this, true);
		unexportObject(rmiRegistry, true);
		System.out.println("Server stopped");
	}

	/**
	 * default constructor
	 * @throws RemoteException
	 * @throws SQLException
	 */
	public ServerImpl() throws RemoteException, SQLException {
		super();
		try {
			SparkUtil.createJavaSparkContext();
			// Check spelling
			Checker.init();
			// Init VietSentidata
			VietSentiData.init();
			// facebook database process
			this.fbDatabase = new FBDatabaseProcess();

			// init data for classify sentiment
			ClassifySentiment.createClassify();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String hello() throws RemoteException {
		return "Server " + IP_THIS_SERVER + " say: Hi client!";
	}

	@Override
	public void processLDA(FacebookData inputDataForLDA) {
		long startTime = System.currentTimeMillis();

		LDAProcess.mainProcessLDA(inputDataForLDA);
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		
		logger.info("Time to run LDA process: " + TimeUnit.MILLISECONDS.toSeconds(totalTime));
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
				TextValueWordCloud ns = new TextValueWordCloud(entry.getKey(), entry.getValue());
				tp.getTextValues().add(ns);
			}
			listTP.add(tp);
		}
		
		return listTP;
	}

	/**
	 * Calling method to get data display report about topicID
	 */
	@Override
	public List<ListReportData> processSentiment(int topicID) throws RemoteException {
		long startTime = System.currentTimeMillis();
		/**
		 * Create return data
		 */
		List<ListReportData> affterSentiment = new ArrayList<ListReportData>(); 
		
		/**
		 * Create instance of sentimentProcess
		 */
		SentimentProcess sentimentProcess = new SentimentProcess();
		/**
		 * Analysis and return to data
		 */
		Map<Integer, StatusAndListComment> fbDataForSentiment = LDAProcess.getFbDataForSentiment(topicID);
		
		affterSentiment = sentimentProcess.processSentiment(fbDataForSentiment);
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		logger.info("Time to run get data for Analysis: " + TimeUnit.MILLISECONDS.toSeconds(totalTime));
		
		return affterSentiment;
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
		return fbDatabase.getFBDataByPageIDAndDate(lstPageID, startDate, endDate);
	}

	@Override
	public void savePageInfo(List<Page_Info> listFanPage)
			throws RemoteException {
		fbDatabase.savePageInfo(listFanPage);
	}

	@Override
	public List<Page_Info> getListPageInfo() throws RemoteException {
		return fbDatabase.getListPageInfo();
	}

	@Override
	public Page_Info getPageInfoByPageID(String pageID) throws RemoteException {
		return fbDatabase.getPageInfoByPageID(pageID);
	}
}
