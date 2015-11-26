package app.utils.dto;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This class content Facebook data <Status, <List of comment>
 * @author thaint
 *
 */
public class FacebookData implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/**
	 * This is main data
	 */
	private Map<String, List<String>> fbDataForService;

	/**
	 * getFbDataForService
	 * @return Map<String, List<String>>
	 */
	public Map<String, List<String>> getFbDataForService() {
		return fbDataForService;
	}

	/**
	 * setFbDataForService
	 * @param fbDataForService Map<String, List<String>>
	 */
	public void setFbDataForService(Map<String, List<String>> fbDataForService) {
		this.fbDataForService = fbDataForService;
	}
}
