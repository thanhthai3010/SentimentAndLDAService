package app.utils.dto;

import java.io.Serializable;

public class TextValue implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String text;
	private Double value;

	public String getText() {
		return text;
	}

	public TextValue() {
	}

	public TextValue(String text, Double value) {
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
