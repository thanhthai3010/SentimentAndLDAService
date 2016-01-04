package app.utils.dto;

import java.io.Serializable;

/**
 * TABLE PAGE_INFOR
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
	 * page urlImage
	 */
	private String urlImage;
	
	/**
	 * page urlImage
	 */
	private String about;

	/**
	 * page description
	 */
	private String description;
	
	/**
	 * page website
	 */
	private String website;
	
	
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
	
	public Page_Info(Long pageID, String pageName, String urlImage,
			String about, String description, String website) {
		super();
		this.pageID = pageID;
		this.pageName = pageName;
		this.urlImage = urlImage;
		this.about = about;
		this.description = description;
		this.website = website;
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

	/**
	 * get getUrlImage
	 * @return urlImage
	 */
	public String getUrlImage() {
		return urlImage;
	}

	/**
	 * set urlImage
	 * @param urlImage
	 */
	public void setUrlImage(String urlImage) {
		this.urlImage = urlImage;
	}

	/**
	 * get page about
	 * @return about
	 */
	public String getAbout() {
		return about;
	}

	/**
	 * set page about
	 * @param about
	 */
	public void setAbout(String about) {
		this.about = about;
	}

	/**
	 * get description
	 * @return description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * set page description
	 * @param description
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * get page website
	 * @return website
	 */
	public String getWebsite() {
		return website;
	}

	/**
	 * set page website
	 * @param website
	 */
	public void setWebsite(String website) {
		this.website = website;
	}

}
