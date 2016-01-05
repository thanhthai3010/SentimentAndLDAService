package app.utils.dto;

import java.io.Serializable;
import java.util.Map;

public class FacebookDataDemo implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * This is main data
	 */
	private Map<Integer, StatusAndListComment> fbDataForService;

	/**
	 * getFbDataForService <Post data, List Of Comments>
	 * 
	 * @return Map<String, List<String>>
	 */
	public Map<Integer, StatusAndListComment> getFbDataForService() {
		return fbDataForService;
	}

	/**
	 * setFbDataForService
	 * 
	 * @param fbDataForService
	 *            Map<String, List<String>>
	 */
	public void setFbDataForService(Map<Integer, StatusAndListComment> fbDataForService) {
		this.fbDataForService = fbDataForService;
	}
}
