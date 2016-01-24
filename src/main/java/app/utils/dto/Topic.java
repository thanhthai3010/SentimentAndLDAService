package app.utils.dto;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Class represent for each Topic <TopicID, list words of this Topic and percent>
 * @author thaint
 *
 */
public class Topic implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	/** topic ID */
	private int idTopic;
	
	/** value to draw Word-Cloud */
	private List<TextValueWordCloud> textValues;
	
	//qtran
	private String titleTooltip;

	public Topic(int idTopic) {
		this.idTopic = idTopic;
		this.setTextValues(new ArrayList<TextValueWordCloud>());
	}

	public int getIdTopic() {
		return idTopic;
	}

	public void setIdTopic(int idTopic) {
		this.idTopic = idTopic;
	}

	public List<TextValueWordCloud> getTextValues() {
		return textValues;
	}

	public void setTextValues(List<TextValueWordCloud> textValues) {
		this.textValues = textValues;
	}

	public String getTitleTooltip() {
		return titleTooltip;
	}

	public void setTitleTooltip(String titleTooltip) {
		this.titleTooltip = titleTooltip;
	}
}
