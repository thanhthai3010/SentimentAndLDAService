package app.server.handling;

import java.rmi.Remote;
import java.rmi.RemoteException;

import app.utils.dto.FacebookData;
import app.utils.dto.ListPieData;
import app.utils.dto.ListTopic;


public interface ServerInterf extends Remote {

	public String hello() throws RemoteException;

	public double runAnalyzeSentiment(String inputText, boolean isNeedToCheck) throws RemoteException;
	public String[] runSpellCheckAndToken(String inputText) throws RemoteException;
	
	//TODO
	/**
	 * Processing data for LDA Model
	 * @param input List of post and comment
	 * @throws RemoteException
	 */
	public void processLDA(FacebookData input) throws RemoteException;
	
	/**
	 * get describle of each topic to draw Word-Cloud
	 * @return
	 * @throws RemoteException
	 */
	public ListTopic getDescribleTopics() throws RemoteException;
	
	public ListPieData processSentiment(int topicID) throws RemoteException;
}
