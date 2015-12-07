package app.utils.dto;

import java.io.Serializable;
import java.util.List;

import com.google.gson.Gson;

public class ListReportData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * display report of status
	 */
	private ReportData statusData;
	
	/**
	 * display report of list comment
	 */
	private List<ReportData> listCommentData;
	
	private int sentimentType;

	/**
	 * default constructor
	 */
	public ListReportData() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * constructor with parameter
	 * @param statusData
	 * @param listCommentData
	 */
	public ListReportData(ReportData statusData, List<ReportData> listCommentData, int sentimentType) {
		super();
		this.statusData = statusData;
		this.listCommentData = listCommentData;
		this.sentimentType = sentimentType;
	}

	/**
	 * get statusData
	 * @return statusData
	 */
	public ReportData getStatusData() {
		return statusData;
	}

	/**
	 * set statusData
	 * @param statusData
	 */
	public void setStatusData(ReportData statusData) {
		this.statusData = statusData;
	}

	/**
	 * get list listCommentData
	 * @return listCommentData
	 */
	public List<ReportData> getListCommentData() {
		return listCommentData;
	}

	/**
	 * set listCommentData
	 * @param listCommentData
	 */
	public void setListCommentData(List<ReportData> listCommentData) {
		this.listCommentData = listCommentData;
	}
	
	/**
	 * get the sum of sentiment type
	 * @return sentimentType
	 */
	public int getSentimentType() {
		return sentimentType;
	}
	
	/**
	 * set sentimentType
	 * @param sentimentType
	 */
	public void setSentimentType(int sentimentType) {
		this.sentimentType = sentimentType;
	}

	/**
	 * convert object to JSon String
	 * @param input
	 * @return
	 */
	public static String toJson(List<ListReportData> input) {
		return new Gson().toJson(input);
	}

}
