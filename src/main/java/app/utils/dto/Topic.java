package app.utils.dto;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Topic implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int idTopic;
	private List<TextValue> textValues;

	public Topic(int idTopic) {
		this.idTopic = idTopic;
		this.setTextValues(new ArrayList<TextValue>());
	}

	public int getIdTopic() {
		return idTopic;
	}

	public void setIdTopic(int idTopic) {
		this.idTopic = idTopic;
	}

	public List<TextValue> getTextValues() {
		return textValues;
	}

	public void setTextValues(List<TextValue> textValues) {
		this.textValues = textValues;
	}
}
