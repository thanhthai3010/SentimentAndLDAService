package app.utils.dto;

import java.io.Serializable;

/**
 * TABLE PAGE_INFO
 * @author thaint
 *
 */
public class Page_Info implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * pageID of facebook page
	 */
	private Long pageID;
	
	/**
	 * page name
	 */
	private String pageName;

	/**
	 * default constructor
	 */
	public Page_Info() {
	}

	/**
	 * Constructor with parameter
	 * @param pageID
	 * @param pageName
	 */
	public Page_Info(Long pageID, String pageName) {
		this.pageID = pageID;
		this.pageName = pageName;
	}

	/**
	 * get pageID
	 * @return pageID
	 */
	public Long getPageID() {
		return pageID;
	}

	/**
	 * set pageID
	 * @param pageID
	 */
	public void setPageID(Long pageID) {
		this.pageID = pageID;
	}

	/**
	 * get pageName
	 * @return pageName
	 */
	public String getPageName() {
		return pageName;
	}

	/**
	 * set pageName
	 * @param pageName
	 */
	public void setPageName(String pageName) {
		this.pageName = pageName;
	}

}
