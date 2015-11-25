package app.utils.dto;

import java.io.Serializable;
import java.util.List;

/**
 * this class containt status and comment data for LDA processing
 * @author thaint
 *
 */
public class InputDataForLDA implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	/** List<String> of status and comment FB */
	private List<String> listOfPostFBForLDA;
	
	/**
	 * getListOfPostFB
	 * @return List<String>
	 */
	public List<String> getListOfPostFBForLDA() {
		return listOfPostFBForLDA;
	}
	
	/**
	 * setListOfPostFB
	 * @param listOfPostFB
	 */
	public void setListOfPostFBForLDA(List<String> listOfPostFBForLDA) {
		this.listOfPostFBForLDA = listOfPostFBForLDA;
	}


}
