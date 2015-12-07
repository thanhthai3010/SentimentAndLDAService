package app.utils.dto;

import java.io.Serializable;
import java.util.List;

import com.google.gson.Gson;

public class ListReportData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ReportData statusData;
	private List<ReportData> listCommentData;

	public ListReportData() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ListReportData(ReportData statusData, List<ReportData> listCommentData) {
		super();
		this.statusData = statusData;
		this.listCommentData = listCommentData;
	}

	public ReportData getStatusData() {
		return statusData;
	}

	public void setStatusData(ReportData statusData) {
		this.statusData = statusData;
	}

	public List<ReportData> getListCommentData() {
		return listCommentData;
	}

	public void setListCommentData(List<ReportData> listCommentData) {
		this.listCommentData = listCommentData;
	}
	
	public static String toJson(List<ListReportData> input) {
		return new Gson().toJson(input);
	}

}
