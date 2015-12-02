package app.utils.dto;

import java.io.Serializable;

public class Post_Data implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long pageID;
	private int postID;
	private String postContent;
	private String dateTime;
	
	public Post_Data() {
	}
	public Post_Data(Long pageID, int postID, String postContent, String dateTime) {
		super();
		this.pageID = pageID;
		this.postID = postID;
		this.postContent = postContent;
		this.setDateTime(dateTime);
	}
	public Long getPageID() {
		return pageID;
	}
	public void setPageID(Long pageID) {
		this.pageID = pageID;
	}
	public int getPostID() {
		return postID;
	}
	public void setPostID(int postID) {
		this.postID = postID;
	}
	public String getPostContent() {
		return postContent;
	}
	public void setPostContent(String postContent) {
		this.postContent = postContent;
	}
	public String getDateTime() {
		return dateTime;
	}
	public void setDateTime(String dateTime) {
		this.dateTime = dateTime;
	}
	
}
