package app.utils.dto;

import java.io.Serializable;

/**
 * data to draw list of comment below
 * @author thaint
 *
 */
public class ReportData implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/** type of Color base on possitive, negative or neutrial  */
	private int typeColor;
	
	/** content of comment */
	private String contentData;

	/**
	 * default constructor
	 */
	public ReportData() {
	}

	/**
	 * Constructor with parameter
	 * @param tyleColor
	 * @param contentData
	 */
	public ReportData(int typeColor, String contentData) {
		this.typeColor = typeColor;
		this.contentData = contentData;
	}

	public int getTypeColor() {
		return typeColor;
	}

	public void setTypeColor(int typeColor) {
		this.typeColor = typeColor;
	}

	public String getContentData() {
		return contentData;
	}

	public void setContentData(String contentData) {
		this.contentData = contentData;
	}
	
}
