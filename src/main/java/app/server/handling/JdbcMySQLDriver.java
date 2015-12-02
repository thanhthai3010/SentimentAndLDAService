package app.server.handling;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcMySQLDriver {

	private static Connection connection = null;
	
	public static Connection getConnetion() {
		try {
			// the mysql driver string
			Class.forName("com.mysql.jdbc.Driver");

			// the mysql url
			String url = "jdbc:mysql://localhost/facebook_data?useUnicode=true&characterEncoding=UTF-8";

			// get the mysql database connection
			connection = DriverManager.getConnection(url, "root", "123");

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connection;
	}
	
	public static void closeConn() throws SQLException {
		if (connection != null) {
			connection.close();
			connection = null;
		}
	}
	
	public static PreparedStatement getPrepareStm(String sql) {
		PreparedStatement preparedStatement = null;
		try {
			Connection cnn = JdbcMySQLDriver.getConnetion();
			preparedStatement = cnn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return preparedStatement;
	}
}
