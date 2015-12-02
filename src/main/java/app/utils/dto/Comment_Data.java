package app.utils.dto;

import java.io.Serializable;

public class Comment_Data implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Long pageID;
	private int postID;
	private int commentID;
	private String commentContent;

	public Comment_Data() {
	}

	public Comment_Data(Long pageID, int postID, int commentID,
			String commentContent) {
		super();
		this.pageID = pageID;
		this.postID = postID;
		this.commentID = commentID;
		this.commentContent = commentContent;
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

	public int getCommentID() {
		return commentID;
	}

	public void setCommentID(int commentID) {
		this.commentID = commentID;
	}

	public String getCommentContent() {
		return commentContent;
	}

	public void setCommentContent(String commentContent) {
		this.commentContent = commentContent;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}
