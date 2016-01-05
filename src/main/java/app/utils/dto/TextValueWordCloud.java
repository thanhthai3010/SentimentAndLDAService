package app.utils.dto;

import java.io.Serializable;

/**
 * Data to draw Word-Cloud
 * @author thaint
 *
 */
public class TextValueWordCloud implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/** text value */
	private String text;
	
	/** percent of each text */
	private Double value;

	public String getText() {
		return text;
	}

	/**
	 * default constructor
	 */
	public TextValueWordCloud() {
	}

	/**
	 * Constructor with parameter
	 * @param text
	 * @param value
	 */
	public TextValueWordCloud(String text, Double value) {
		super();
		this.text = text;
		this.value = value;
	}

	public void setText(String text) {
		this.text = text;
	}

	public Double getValue() {
		return value;
	}

	public void setValue(Double value) {
		this.value = value;
	}

}
