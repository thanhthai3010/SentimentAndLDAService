package app.utils.dto;

import java.io.Serializable;
import java.util.List;

public class PieChart implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * label of each pie
	 */
	private String label;
	
	/**
	 * int percent values
	 */
	private int percent;

	/**
	 * default constructor
	 */
	public PieChart() {
	}

	/**
	 * constructor with parameter
	 * @param label
	 * @param percent
	 */
	public PieChart(String label, int percent) {
		this.label = label;
		this.percent = percent;
	}

	/**
	 * get label
	 * @return label
	 */
	public String getLabel() {
		return label;
	}

	/**
	 * set label
	 * @param label
	 */
	public void setLabel(String label) {
		this.label = label;
	}

	/**
	 * get percent
	 * @return percent
	 */
	public int getPercent() {
		return percent;
	}

	/**
	 * set percent
	 * @param percent
	 */
	public void setPercent(int percent) {
		this.percent = percent;
	}

	/**
	 * convert this object to Json
	 * @param inputPieChart
	 * @return JSON
	 */
	public static String pieChartsToJson(List<PieChart> inputPieChart){
		String result = "";
		
		result +="[[\"Label\", \"Percent\"],";
		for (int i = 0; i < inputPieChart.size(); i++) {
			result += String.format("[\"%s\", %d]", inputPieChart.get(i).getLabel(), inputPieChart.get(i).getPercent());
			if (i < inputPieChart.size() - 1) {
				result += ",";
			}
		}
		result += "]";
		
		return result;
	}
}
