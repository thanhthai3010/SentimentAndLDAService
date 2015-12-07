package app.utils.dto;

import java.io.Serializable;
import java.util.ArrayList;

import com.google.gson.Gson;

/**
 * List all of report object
 * @author thaint
 *
 */
public class ListReportData extends ArrayList<ReportData> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public String pieDataToJson(){
		return new Gson().toJson(this);
	}

}
