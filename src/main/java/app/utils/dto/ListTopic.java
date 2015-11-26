package app.utils.dto;
import java.io.Serializable;
import java.util.ArrayList;

import com.google.gson.Gson;


/**
 * List class content data of Topics, for draw Word-Cloud
 * @author thaint
 *
 */
public class ListTopic extends ArrayList<Topic> implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public String toTopicsJson(){
		return new Gson().toJson(this);
	}
	
}
