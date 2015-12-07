package app.server.handling;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import app.utils.dto.FacebookData;
import app.utils.dto.FacebookDataToInsertDB;
import app.utils.dto.ListReportData;
import app.utils.dto.ListTopic;
import app.utils.dto.Page_Info;


public interface ServerInterf extends Remote {

	public String hello() throws RemoteException;
	
	/**
	 * thaint
	 * Save facebook data into database
	 * @param fbDataToInsertDB
	 * @throws RemoteException
	 */
	public void saveFBData(FacebookDataToInsertDB fbDataToInsertDB) throws RemoteException;
	
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
	
	/**
	 * Start sentiment process using topic ID
	 * @param topicID
	 * @return
	 * @throws RemoteException
	 */
	public List<ListReportData> processSentiment(int topicID) throws RemoteException;
	
	/**
	 * This function will get data of facebook using list pageID input and dates
	 * @param lstPageID list page ID
	 * @param startDate date
	 * @param endDate date
	 * @return
	 * @throws RemoteException
	 */
	public FacebookData getFBDataByPageIDAndDate(List<String> lstPageID, String startDate,
			String endDate) throws RemoteException;
	
	/**
	 * Save facebook page info
	 * @param listFanPage
	 * @throws RemoteException
	 */
	public void savePageInfo(List<Page_Info> listFanPage) throws RemoteException;
	
	/**
	 * get all of facebook page info
	 * @param listFanPage
	 * @throws RemoteException
	 */
	public List<Page_Info> getListPageInfo() throws RemoteException;
}
