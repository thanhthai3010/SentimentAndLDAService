package app.utils.dto;

import java.io.Serializable;
import java.util.ArrayList;

import com.google.gson.Gson;

public class ListPieData extends ArrayList<PieData> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public String pieDataToJson(){
		return new Gson().toJson(this);
	}

}
