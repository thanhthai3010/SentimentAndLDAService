package app.utils.dto;

import java.io.Serializable;
import java.util.List;

public class StatusAndListComment implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String status;
	private List<String> listComment;
	
	public StatusAndListComment() {
		super();
		// TODO Auto-generated constructor stub
	}
	public StatusAndListComment(String status, List<String> listComment) {
		super();
		this.status = status;
		this.listComment = listComment;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public List<String> getListComment() {
		return listComment;
	}
	public void setListComment(List<String> listComment) {
		this.listComment = listComment;
	}
	

}
