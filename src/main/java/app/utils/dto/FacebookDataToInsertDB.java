package app.utils.dto;

import java.io.Serializable;
import java.util.List;

public class FacebookDataToInsertDB implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private List<Post_Data> listPost_Data;

	private List<Comment_Data> listComment_Data;

	private String[] listPageID;

	public FacebookDataToInsertDB() {
	}

	public FacebookDataToInsertDB(List<Post_Data> listPost_Data,
			List<Comment_Data> listComment_Data, String[] listPageID) {
		super();
		this.listPost_Data = listPost_Data;
		this.listComment_Data = listComment_Data;
		this.listPageID = listPageID;
	}

	public List<Post_Data> getListPost_Data() {
		return listPost_Data;
	}

	public void setListPost_Data(List<Post_Data> listPost_Data) {
		this.listPost_Data = listPost_Data;
	}

	public List<Comment_Data> getListComment_Data() {
		return listComment_Data;
	}

	public void setListComment_Data(List<Comment_Data> listComment_Data) {
		this.listComment_Data = listComment_Data;
	}

	public String[] getListPageID() {
		return listPageID;
	}

	public void setListPageID(String[] listPageID) {
		this.listPageID = listPageID;
	}

}
