package app.server.handling;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import app.utils.dto.FacebookData;
import app.utils.dto.FacebookDataToInsertDB;
import app.utils.dto.ListPieData;
import app.utils.dto.ListTopic;


public interface ServerInterf extends Remote {

	public String hello() throws RemoteException;

	/**
	 * thaint
	 * Save facebook data into database
	 * @param fbDataToInsertDB
	 * @throws RemoteException
	 */
	public void saveFBData(FacebookDataToInsertDB fbDataToInsertDB) throws RemoteException;
	
	public double runAnalyzeSentiment(String inputText, boolean isNeedToCheck) throws RemoteException;	
	
	public String[] runSpellCheckAndToken(String inputText) throws RemoteException;
	
	/**
	 * thaint
	 * Processing data for LDA Model
	 * @param input List of post and comment
	 * @throws RemoteException
	 */
	public void processLDA(FacebookData input) throws RemoteException;
	
	/**
	 * thaint
	 * Get list topic to draw word-cloud
	 * @return ListTopic
	 * @throws RemoteException
	 */
	public ListTopic getDescribleTopics() throws RemoteException;
	
	public ListPieData processSentiment(int topicID) throws RemoteException;
	
	public FacebookData getFBDataByPageIDAndDate(List<String> lstPageID, String startDate,
			String endDate) throws RemoteException;
}
