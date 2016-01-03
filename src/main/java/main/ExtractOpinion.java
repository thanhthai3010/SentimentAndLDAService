package main;

import java.rmi.RemoteException;

import app.server.handling.ServerImpl;

public class ExtractOpinion {
	
	//qtran
	public static final String RESOURCE_PATH = "./DictionaryData/";

	public static void main(String[] args) {

		try {
			ServerImpl server = new ServerImpl();
			server.start();
			
		} catch (RemoteException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
