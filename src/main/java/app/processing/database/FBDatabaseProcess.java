package app.processing.database;

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
				preparedStatement.setString(3, Checker.correctUnicodeCharacters(post_Data.getPostContent()));
				
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
				preparedStatement.setString(4, Checker.correctUnicodeCharacters(cmData.getCommentContent()));

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
	 * This function provide a way to get facebook data from database using pageID and date.
	 * @param lstPageID List of page
	 * @param startDate String date
	 * @param endDate String date
	 * @return
	 */
	public FacebookData getFBDataByPageIDAndDate(List<String> lstPageID, String startDate,
			String endDate) {

		// return data
		FacebookData fbDataFromDB = new FacebookData();

		// PreparedStatement
		PreparedStatement preparedStatement = null;

		/**
		 * This variable stored data of <Post, List<Comment>>
		 */
		Map<String, List<String>> fbDataForService = new LinkedHashMap<String, List<String>>();

		// loop all pageID input
		for (String pageID : lstPageID) {
			String getTableSQL = "SELECT A.PAGE_ID, A.POST_ID, A.POST_CONTENT, B.COMMENT_ID, B.COMMENT_CONTENT "
					+ " FROM POST_DATA A "
					+ " LEFT JOIN COMMENT_DATA B "
					+ " ON B.PAGE_ID = A.PAGE_ID "
					+ " AND B.POST_ID = A.POST_ID "
					+ " WHERE "
					+ " A.PAGE_ID = ?" + " AND A.DATE_TIME BETWEEN ? and ?"
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
				while (rs.next()) {
					if (postID.equals("") || postID.equals(rs.getString("POST_ID"))) {
						postContent = rs.getString("POST_CONTENT");
						String cmContent = rs.getString("COMMENT_CONTENT");
						if (cmContent != null) {
							lstComment.add(cmContent);
						}
					} else {
						// Put to hashMap
						fbDataForService.put(postContent, lstComment);
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
	 * @param listFanPage
	 * @throws RemoteException
	 */
	public void savePageInfo(List<Page_Info> listFanPage) {
		// PreparedStatement
		PreparedStatement preparedStatement = null;
		for (Page_Info pageInfo : listFanPage) {
			String insertTableSQL = "INSERT INTO PAGE_INFO"
					+ " (PAGE_ID, PAGE_NAME) VALUES" + " (?, ?)";

			try {
				preparedStatement = JdbcMySQLDriver
						.getPrepareStm(insertTableSQL);

				// set page ID
				preparedStatement.setLong(1, pageInfo.getPageID());

				// Set page name
				preparedStatement.setString(2, pageInfo.getPageName());
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
	}
	
	/**
	 * get all of pageInfo in database to display web
	 * @return
	 */
	public List<Page_Info> getListPageInfo() {

		List<Page_Info> listAllPageInfo = new ArrayList<Page_Info>();
 		// PreparedStatement
		PreparedStatement preparedStatement = null;
		
		String getTableSQL = "SELECT PAGE_ID, PAGE_NAME"
				+ " FROM PAGE_INFO";
		
		try {
			preparedStatement = JdbcMySQLDriver.getPrepareStm(getTableSQL);
			
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				Page_Info pageInfo = new Page_Info(rs.getLong("PAGE_ID"), rs.getString("PAGE_NAME"));
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
	
//	public static void main(String[] args) {
//		JdbcMySQLDriver.getConnetion();
//		FBDatabaseProcess fbd = new FBDatabaseProcess();
//		List<String> a = new ArrayList<String>();
//		a.add("2311");
//		List<Page_Info> fbdx = fbd.getListPageInfo();
//		for (Page_Info page_Info : fbdx) {
//			logger.info(page_Info.getPageID().toString());
//		}
//	}
}
