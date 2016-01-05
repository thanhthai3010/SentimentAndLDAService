package app.process.database;

import java.rmi.RemoteException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import app.process.spellcheker.Checker;
import app.server.handling.JdbcMySQLDriver;
import app.utils.dto.Comment_Data;
import app.utils.dto.FacebookData;
import app.utils.dto.FacebookDataToInsertDB;
import app.utils.dto.Page_Info;
import app.utils.dto.Post_Data;
import app.utils.dto.StatusAndListComment;

public class FBDatabaseProcess {

	/**
	 * List store pageID input
	 */
	private String[] listPageID;

	/**
	 * facebook data to insert into database
	 */
	private FacebookDataToInsertDB fbDataToInsertDB;

	private static final Logger logger = LoggerFactory
			.getLogger(FBDatabaseProcess.class);

	/**
	 * Default constructor
	 */
	public FBDatabaseProcess() {

	}

	/**
	 * Constructor with data
	 * 
	 * @param fbDataToInsertDB
	 */
	public FBDatabaseProcess(FacebookDataToInsertDB fbDataToInsertDB) {
		this.fbDataToInsertDB = fbDataToInsertDB;
		this.listPageID = fbDataToInsertDB.getListPageID();
	}

	/**
	 * Delete all of records in table POST_DATA has pageID
	 */
	private void deletePostDataWithPageID() {
		PreparedStatement preparedStatement = null;
		for (String pageID : this.listPageID) {

			String insertTableSQL = "DELETE FROM POST_DATA "
					+ " WHERE PAGE_ID = ?";

			try {
				preparedStatement = JdbcMySQLDriver
						.getPrepareStm(insertTableSQL);

				preparedStatement.setLong(1, Long.parseLong(pageID));
				// execute SQL stetement
				preparedStatement.executeUpdate();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.info(e.getMessage());
			} finally {
				if (preparedStatement != null)
					try {
						preparedStatement.close();
						JdbcMySQLDriver.closeConn();
					} catch (SQLException logOrIgnore) {
					}
			}
		}
		logger.info("Done delete POST_DATA with PageID");
	}

	/**
	 * Delete all of record in table COMMENT_DATA has pageID
	 */
	private void deleteCommentDataWithPageID() {
		PreparedStatement preparedStatement = null;
		for (String pageID : this.listPageID) {

			String insertTableSQL = "DELETE FROM COMMENT_DATA "
					+ " WHERE PAGE_ID = ?";

			try {
				preparedStatement = JdbcMySQLDriver
						.getPrepareStm(insertTableSQL);

				preparedStatement.setLong(1, Long.parseLong(pageID));
				// execute SQL stetement
				preparedStatement.executeUpdate();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.info(e.getMessage());
			} finally {
				if (preparedStatement != null)
					try {
						preparedStatement.close();
						JdbcMySQLDriver.closeConn();
					} catch (SQLException logOrIgnore) {
					}
			}
		}
		logger.info("Done delete COMMENT_DATA with PageID");
	}

	/**
	 * Delete all of record in table PAGE_INFO has pageID
	 */
	private void deletePageInfo(Long pageID) {
		PreparedStatement preparedStatement = null;

		String insertTableSQL = "DELETE FROM PAGE_INFO WHERE PAGE_ID = ?";

		try {
			preparedStatement = JdbcMySQLDriver.getPrepareStm(insertTableSQL);

			preparedStatement.setLong(1, pageID);
			// execute SQL stetement
			preparedStatement.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.info(e.getMessage());
		} finally {
			if (preparedStatement != null)
				try {
					preparedStatement.close();
					JdbcMySQLDriver.closeConn();
				} catch (SQLException logOrIgnore) {
				}
		}

		logger.info("Done delete PAGE_INFO with PageID");
	}

	/**
	 * Save post data into table POST_DATA
	 * 
	 * @param pageID
	 * @throws SQLException
	 */
	private void savePostData(List<Post_Data> lstPostData) {

		// delete Post data
		deletePostDataWithPageID();

		PreparedStatement preparedStatement = null;
		for (Post_Data post_Data : lstPostData) {

			String insertTableSQL = "INSERT INTO POST_DATA"
					+ " (PAGE_ID, POST_ID, POST_CONTENT, DATE_TIME) VALUES"
					+ " (?, ?, ?, ?)";

			try {
				preparedStatement = JdbcMySQLDriver
						.getPrepareStm(insertTableSQL);

				preparedStatement.setLong(1, post_Data.getPageID());

				preparedStatement.setInt(2, post_Data.getPostID());

				// Convert character to Unicode before insert into database
				preparedStatement.setString(3, Checker
						.correctUnicodeCharacters(post_Data.getPostContent()));

				preparedStatement.setString(4, post_Data.getDateTime());
				// execute insert SQL stetement
				preparedStatement.executeUpdate();
			} catch (SQLException e) {
				logger.info(e.getMessage());
			} finally {
				if (preparedStatement != null)
					try {
						preparedStatement.close();
						JdbcMySQLDriver.closeConn();
					} catch (SQLException logOrIgnore) {
					}
			}
		}
		logger.info("Done Save POST_DATA with PageID");
	}

	/**
	 * Save Comment into table COMMENT_DATA
	 * 
	 * @param pageID
	 * @throws SQLException
	 */
	private void saveCommentData(List<Comment_Data> lstComment) {

		// delete Comment data
		deleteCommentDataWithPageID();

		PreparedStatement preparedStatement = null;
		for (Comment_Data cmData : lstComment) {
			String insertTableSQL = "INSERT INTO COMMENT_DATA"
					+ " (PAGE_ID, POST_ID, COMMENT_ID, COMMENT_CONTENT) VALUES"
					+ " (?, ?, ?, ?)";
			try {
				preparedStatement = JdbcMySQLDriver
						.getPrepareStm(insertTableSQL);

				preparedStatement.setLong(1, cmData.getPageID());

				preparedStatement.setInt(2, cmData.getPostID());

				preparedStatement.setInt(3, cmData.getCommentID());

				// Convert character to Unicode before insert into database
				// TODO: need to remove checker here
				String commentData = Checker.correctUnicodeCharacters(cmData
						.getCommentContent());
				// commentData = Checker.correctSpell(commentData);
				// commentData = Checker.correctSpecialEmoticons(commentData);
				preparedStatement.setString(4, commentData);

				// execute insert SQL stetement
				preparedStatement.executeUpdate();
			} catch (SQLException e) {
				logger.info(e.getMessage());
			} finally {
				if (preparedStatement != null)
					try {
						preparedStatement.close();
						JdbcMySQLDriver.closeConn();
					} catch (SQLException logOrIgnore) {
					}
			}
		}
		logger.info("Done save COMMENT_DATA with PageID");
	}

	/**
	 * Calling main saveFB method
	 */
	public void saveFBData() {
		// After get data, we need to save
		// POST_DATA
		savePostData(fbDataToInsertDB.getListPost_Data());
		// Save Comment DATA
		saveCommentData(fbDataToInsertDB.getListComment_Data());
	}

	/**
	 * This function provide a way to get facebook data from database using
	 * pageID and date.
	 * 
	 * @param lstPageID
	 *            List of page
	 * @param startDate
	 *            String date
	 * @param endDate
	 *            String date
	 * @return
	 */
	public FacebookData getFBDataByPageIDAndDate(List<String> lstPageID,
			String startDate, String endDate) {

		// return data
		FacebookData fbDataFromDB = new FacebookData();

		// PreparedStatement
		PreparedStatement preparedStatement = null;

		/**
		 * This variable stored data of <Post, List<Comment>>
		 */
		Map<Integer, StatusAndListComment> fbDataForService = new LinkedHashMap<Integer, StatusAndListComment>();

		// loop all pageID input
		for (String pageID : lstPageID) {
			String getTableSQL = "SELECT A.PAGE_ID, A.POST_ID, A.POST_CONTENT, B.COMMENT_ID, B.COMMENT_CONTENT "
					+ " FROM POST_DATA A "
					+ " LEFT JOIN COMMENT_DATA B "
					+ " ON B.PAGE_ID = A.PAGE_ID "
					+ " AND B.POST_ID = A.POST_ID "
					+ " WHERE "
					+ " A.PAGE_ID = ?"
					+ " AND A.DATE_TIME BETWEEN ? and ?"
					+ " ORDER BY A.POST_ID";

			try {
				preparedStatement = JdbcMySQLDriver.getPrepareStm(getTableSQL);

				// Set pageID
				preparedStatement.setLong(1, Long.parseLong(pageID));
				// Set startDate
				preparedStatement.setString(2, startDate);
				// Set endDate
				preparedStatement.setString(3, endDate);

				ResultSet rs = preparedStatement.executeQuery();
				List<String> lstComment = new ArrayList<String>();
				// default postID
				String postID = "";
				// default postContent
				String postContent = "";
				// loop for all records
				int index = 0;
				while (rs.next()) {
					if (postID.equals("") || postID.equals(rs.getString("POST_ID"))) {
						postContent = rs.getString("POST_CONTENT");
						String cmContent = rs.getString("COMMENT_CONTENT");
						if (cmContent != null) {
							lstComment.add(cmContent);
						}
					} else {
						// Put to hashMap
						StatusAndListComment sttAndListComment = new StatusAndListComment(postContent, lstComment);
						fbDataForService.put(index, sttAndListComment);
						index++;
						// Break to new record
						lstComment = new ArrayList<String>();
						String cmContent = rs.getString("COMMENT_CONTENT");
						if (cmContent != null) {
							lstComment.add(cmContent);
						}
						postContent = rs.getString("POST_CONTENT");
					}
					// update postID value
					postID = rs.getString("POST_ID");
				}
				
				// Add the last record
				StatusAndListComment sttAndListComment = new StatusAndListComment(postContent, lstComment);
				fbDataForService.put(index, sttAndListComment);

			} catch (NumberFormatException e) {
				logger.info(e.getMessage());
			} catch (SQLException e) {
				logger.info(e.getMessage());
			} finally {
				if (preparedStatement != null)
					try {
						preparedStatement.close();
						JdbcMySQLDriver.closeConn();
					} catch (SQLException logOrIgnore) {
					}
			}
		}
		// set for return data
		fbDataFromDB.setFbDataForService(fbDataForService);
		return fbDataFromDB;
	}

	/**
	 * Save facebook page info
	 * 
	 * @param listFanPage
	 * @throws RemoteException
	 */
	public void savePageInfo(List<Page_Info> listFanPage) {

		// PreparedStatement
		PreparedStatement preparedStatement = null;
		for (Page_Info pageInfo : listFanPage) {

			// delete PAGE_INFO
			deletePageInfo(pageInfo.getPageID());

			String insertTableSQL = "INSERT INTO PAGE_INFO"
					+ " (PAGE_ID, PAGE_NAME, IMAGE, ABOUT, DESCRIPTION, WEBSITE) VALUES"
					+ " (?, ?, ?, ?, ?, ?)";

			try {
				preparedStatement = JdbcMySQLDriver
						.getPrepareStm(insertTableSQL);

				// set page ID
				preparedStatement.setLong(1, pageInfo.getPageID());

				// Set page name
				preparedStatement.setString(2, pageInfo.getPageName());
				
				// Set page image
				preparedStatement.setString(3, pageInfo.getUrlImage());
				
				// Set page about
				preparedStatement.setString(4, pageInfo.getAbout());
				
				// Set page description
				preparedStatement.setString(5, pageInfo.getDescription());
				
				// Set page website
				preparedStatement.setString(6, pageInfo.getWebsite());
				// execute insert SQL stetement
				preparedStatement.executeUpdate();
				logger.info("Done save PAGE_INFO");
			} catch (SQLException e) {
				logger.info(e.getMessage());
			} finally {
				if (preparedStatement != null)
					try {
						preparedStatement.close();
						JdbcMySQLDriver.closeConn();
					} catch (SQLException logOrIgnore) {
					}
			}
		}
	}

	/**
	 * get all of pageInfo in database to display web
	 * 
	 * @return
	 */
	public List<Page_Info> getListPageInfo() {

		List<Page_Info> listAllPageInfo = new ArrayList<Page_Info>();
		// PreparedStatement
		PreparedStatement preparedStatement = null;

		String getTableSQL = "SELECT PAGE_ID, PAGE_NAME, IMAGE, ABOUT, DESCRIPTION, WEBSITE" + " FROM PAGE_INFO";

		try {
			preparedStatement = JdbcMySQLDriver.getPrepareStm(getTableSQL);

			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				Page_Info pageInfo = new Page_Info(rs.getLong("PAGE_ID"),
						rs.getString("PAGE_NAME"), rs.getString("IMAGE"),
						rs.getString("ABOUT"), rs.getString("DESCRIPTION"),
						rs.getString("WEBSITE"));
				listAllPageInfo.add(pageInfo);
			}
		} catch (SQLException e) {
			logger.info(e.getMessage());
		} finally {
			if (preparedStatement != null)
				try {
					preparedStatement.close();
					JdbcMySQLDriver.closeConn();
				} catch (SQLException logOrIgnore) {
				}
		}

		return listAllPageInfo;
	}
	
	/**
	 * get pageInfo in database using pageID
	 * 
	 * @return Page_Info
	 */
	public Page_Info getPageInfoByPageID(String pageID) {

		Page_Info pageInfo = new Page_Info();
		// PreparedStatement
		PreparedStatement preparedStatement = null;

		String getTableSQL = "SELECT PAGE_ID, PAGE_NAME, IMAGE, ABOUT, DESCRIPTION, WEBSITE" + " FROM PAGE_INFO "
				+ " WHERE PAGE_ID = ?";

		try {
			preparedStatement = JdbcMySQLDriver.getPrepareStm(getTableSQL);
			preparedStatement.setLong(1, Long.parseLong(pageID));
			
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				pageInfo = new Page_Info(rs.getLong("PAGE_ID"),
						rs.getString("PAGE_NAME"), rs.getString("IMAGE"),
						rs.getString("ABOUT"), rs.getString("DESCRIPTION"),
						rs.getString("WEBSITE"));
			}
		} catch (SQLException e) {
			logger.info(e.getMessage());
		} finally {
			if (preparedStatement != null)
				try {
					preparedStatement.close();
					JdbcMySQLDriver.closeConn();
				} catch (SQLException logOrIgnore) {
				}
		}

		return pageInfo;
	}

	public List<String> getCommentData() {
		List<String> listComment = new ArrayList<String>();

		PreparedStatement preparedStatement = null;

		String getTableSQL = "SELECT COMMENT_CONTENT" + " FROM COMMENT_DATA "
				+ " WHERE PAGE_ID = 541752015846507";
		preparedStatement = JdbcMySQLDriver.getPrepareStm(getTableSQL);
		ResultSet rs;
		try {
			rs = preparedStatement.executeQuery();

			while (rs.next()) {
				String comment = rs.getString("COMMENT_CONTENT");
				listComment.add(comment);
			}
		} catch (SQLException e) {
			logger.info("Can not get COMMENT_DATA");
		}
		logger.info("Done get COMMENT_DATA");
		return listComment;
	}

	public static void main(String[] args) {
		JdbcMySQLDriver.getConnetion();
		FBDatabaseProcess fbd = new FBDatabaseProcess();
		List<String> lstPageID = new ArrayList<String>();
		lstPageID.add("447498478655695");
		FacebookData data = fbd.getFBDataByPageIDAndDate(lstPageID,
				"2015-12-17", "2016-01-03");
		for (Integer key : data.getFbDataForService().keySet()) {
			System.out.println("-- " + key );
			System.out.println("-- " + data.getFbDataForService().get(key).getListComment() );
		}
	}
}
