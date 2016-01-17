package app.utils.dto;

import java.io.Serializable;

public class Feed_Back implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * content of feed back
	 */
	private String feedBack_Content;
	
	/**
	 * label of feed back
	 */
	private int feedBack_Label;
	
	/**
	 * Default constructor
	 */
	public Feed_Back() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Constructor with parameters
	 * @param feedBack_Content
	 * @param feedBack_Label
	 */
	public Feed_Back(String feedBack_Content, int feedBack_Label) {
		super();
		this.feedBack_Content = feedBack_Content;
		this.feedBack_Label = feedBack_Label;
	}
	
	/**
	 * get feedBack_Content
	 * @return feedBack_Content
	 */
	public String getFeedBack_Content() {
		return feedBack_Content;
	}
	
	/**
	 * set feedBack_Content
	 * @param feedBack_Content
	 */
	public void setFeedBack_Content(String feedBack_Content) {
		this.feedBack_Content = feedBack_Content;
	}
	
	/**
	 * get feedBack_Label
	 * @return feedBack_Label
	 */
	public int getFeedBack_Label() {
		return feedBack_Label;
	}
	
	/**
	 * set feedBack_Label
	 * @param feedBack_Label
	 */
	public void setFeedBack_Label(int feedBack_Label) {
		this.feedBack_Label = feedBack_Label;
	}
}
