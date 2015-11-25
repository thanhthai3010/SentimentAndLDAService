package app.utils.dto;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PieChart implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String label;
	private int percent;

	public PieChart() {
	}

	public PieChart(String label, int percent) {
		this.label = label;
		this.percent = percent;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public int getPercent() {
		return percent;
	}

	public void setPercent(int percent) {
		this.percent = percent;
	}

	public static String pieChartsToJson(List<PieChart> inputPieChart){
		String result = "";
		
		result +="[['Label', 'Percent'],";
		for (int i = 0; i < inputPieChart.size(); i++) {
			result += String.format("['%s', %d]", inputPieChart.get(i).getLabel(), inputPieChart.get(i).getPercent());
			if (i < inputPieChart.size() - 1) {
				result += ",";
			}
		}
		result += "]";
		
		return result;
	}
	
	public static void main(String[] args) {
		List<PieChart> inputPieChart = new ArrayList<PieChart>();
		for (int i = 0; i < 3; i++) {
			PieChart ax = new PieChart("label " + i, i *2);
			inputPieChart.add(ax);
		}
		System.out.println(PieChart.pieChartsToJson(inputPieChart));
	}
}
