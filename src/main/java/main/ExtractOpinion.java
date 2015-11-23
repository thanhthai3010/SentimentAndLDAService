package main;

import java.rmi.RemoteException;

import app.server.handling.ServerImpl;

public class ExtractOpinion {

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
