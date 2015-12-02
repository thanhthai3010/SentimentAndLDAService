package app.utils.dto;

import java.io.Serializable;

/**
 * data to draw list of comment below
 * @author thaint
 *
 */
public class PieData implements Serializable{

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
	public PieData() {
	}

	/**
	 * Constructor with parameter
	 * @param tyleColor
	 * @param contentData
	 */
	public PieData(int tyleColor, String contentData) {
		this.typeColor = tyleColor;
		this.contentData = contentData;
	}

	public int getTypeColor() {
		return typeColor;
	}

	public void setTypeColor(int tyleColor) {
		this.typeColor = tyleColor;
	}

	public String getContentData() {
		return contentData;
	}

	public void setContentData(String contentData) {
		this.contentData = contentData;
	}
	
	public static void main(String[] args) {
		ListPieData listPie = new ListPieData();
		for (int i = 0; i < 8; i++) {
			PieData a = new PieData(i, "this is content of " + i);
			listPie.add(a);
		}
		
		System.out.println(listPie.pieDataToJson());
	}
	
}
