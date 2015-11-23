package vietSentiData;

import java.io.Serializable;

/**
 * RowData is class represent for each row in VietSentiWordNet data
 * @author thaint
 *
 */
public class RowData implements Serializable {

	private static final long serialVersionUID = 1L;
	
	/**
	 * part-of-speech tagging
	 */
	private String pos;
	
	/**
	 * ID of synset
	 */
	private String id;
	
	/**
	 * positive score
	 */
	private double posScore;
	
	/**
	 * negative score
	 */
	private double negScore;
	
	/**
	 * collect of synset
	 */
	private String synsetTerms;
	
	/**
	 * meaning of synsets
	 */
	private String gloss;

	/**
	 * default constructor
	 */
	public RowData() {

	}

	/**
	 * Constructor with parameter
	 * @param pos String
	 * @param id String
	 * @param posScore double
	 * @param negScore double
	 * @param synsetTerms String
	 * @param gloss String
	 */
	public RowData(String pos, String id, double posScore, double negScore,
			String synsetTerms, String gloss) {
		super();
		this.pos = pos;
		this.id = id;
		this.posScore = posScore;
		this.negScore = negScore;
		this.synsetTerms = synsetTerms;
		this.gloss = gloss;
	}

	/**
	 * get part-of-speech tagging
	 * @return pos String
	 */
	public String getPos() {
		return pos;
	}

	/**
	 * set part-of-speech tagging
	 * @param pos String
	 */
	public void setPos(String pos) {
		this.pos = pos;
	}

	/**
	 * get ID of synset
	 * @return id String
	 */
	public String getId() {
		return id;
	}

	/**
	 * set ID of synset
	 * @param id String
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * get positive score
	 * @return posScore double
	 */
	public double getPosScore() {
		return posScore;
	}

	/**
	 * set positive score
	 * @param posScore double
	 */
	public void setPosScore(double posScore) {
		this.posScore = posScore;
	}

	/**
	 * get negative score
	 * @return negScore double
	 */
	public double getNegScore() {
		return negScore;
	}

	/**
	 * set negative score
	 * @param negScore double
	 */
	public void setNegScore(double negScore) {
		this.negScore = negScore;
	}

	/**
	 * get collect of synset
	 * @return synsetTerms String
	 */
	public String getSynsetTerms() {
		return synsetTerms;
	}

	/**
	 * set collect of synset
	 * @param synsetTerms String
	 */
	public void setSynsetTerms(String synsetTerms) {
		this.synsetTerms = synsetTerms;
	}

	/**
	 * get meaning of synsets 
	 * @return gloss String
	 */
	public String getGloss() {
		return gloss;
	}

	/**
	 * set meaning of synsets 
	 * @param gloss String
	 */
	public void setGloss(String gloss) {
		this.gloss = gloss;
	}
}
