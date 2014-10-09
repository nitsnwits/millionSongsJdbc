/**
 * Provides one JDBC connection to the database
 */
package resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import org.json.simple.JSONObject;

/**
 * @author neerajsharma, sameersave
 *
 */
public class JDBCConnection {
	private Connection jdbcConnection;
	
	public Connection getConnection() {
		
		//check for postgresql drivers
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			Log.logger.error("Postgresql driver not loaded.");
		}
		
		//Read database config from config file
		ReadConfig config = new ReadConfig("config/server.conf");

		//read server and postgresql config
		JSONObject postgreSqlConfig = config.get("postgresql");
		JSONObject serverConfig = config.get("server");
		
		//set JSON keys that are being read from server.conf
		String pgsqlUrl = "url";
		String pgsqlUsername = "username";
		String pgsqlPassword = "password";
		String pgsqlAutoCommit = config.get(postgreSqlConfig, "autoCommit");
		boolean autoCommit = true;
		if (pgsqlAutoCommit.equalsIgnoreCase("false")) {
			autoCommit = false;
		} else if (pgsqlAutoCommit.equalsIgnoreCase("true")) {
			autoCommit = true;
		} else {
			Log.logger.warn("No autocommit configuration set, using default: true");
		}
		
		//try to connect to databases
		//TODO: Read he postgresql config from server.conf using ReadConfig class -> Done
		//jdbcConnection = null;
		try {
			this.jdbcConnection = DriverManager.getConnection(config.get(postgreSqlConfig, pgsqlUrl), config.get(postgreSqlConfig, pgsqlUsername), config.get(postgreSqlConfig, pgsqlPassword));
			this.jdbcConnection.setAutoCommit(autoCommit);
		} catch (SQLException e) {
			Log.logger.error("Unable to load connection to database.");
		}
		return this.jdbcConnection;
	}
	
	public void commit() {
		try {
			this.jdbcConnection.commit();
		} catch (SQLException e) {
			Log.logger.error("Unable to commit to database.");
		}
	}
	
	public void insert(HashMap<String, String> row) {
		try {
			StringBuilder sqlInsert = new StringBuilder();
			sqlInsert.append("INSERT INTO amazon_reviews (");
			sqlInsert.append("product_id, title, price, ");
			sqlInsert.append("user_id, profile_name, helpfulness, score, review_time, review_summary ) values (");
			
			String product_id = "'" + row.get("productId") + "', ";
			String title = "'" + row.get("title") + "', ";
			String price = "'" + row.get("price") + "', ";
			String user_id = "'" + row.get("userId") + "', ";
			String profile_name = "'" + row.get("profileName") + "', ";
			String helpfulness = "'" + row.get("helpfulness") + "', ";
			String review_summary = "'" + row.get("summary") + "')";
			
			sqlInsert.append(product_id + title + price + user_id + profile_name + helpfulness + row.get("score") + ", " + row.get("time") + ", " + review_summary);

			//Log.logger.info("insert: " + sqlInsert.toString());
			
			PreparedStatement preparedStatement = this.jdbcConnection.prepareStatement(sqlInsert.toString());

			preparedStatement.executeUpdate();
			preparedStatement.close();

		} catch (SQLException e) {
			e.printStackTrace();
			Log.logger.error("Unable to insert to database");
		} catch (NullPointerException e) {
			e.getCause();
			Log.logger.error("Got null pointer exception.");
		}
	}
	
	public void createTable(HashMap<String, String> tableHash, String tableName) {
		//drop the table if exists
		StringBuilder sqlDrop = new StringBuilder();
		sqlDrop.append("DROP TABLE IF EXISTS " + tableName);
		//use sb to create create table statement and execute it using the associated connection
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE " + tableName + " ( ");
		for (String columnName: tableHash.keySet()) {
			sql.append(columnName + " " + tableHash.get(columnName) + ", ");
		}
		//remove trailing comma and space
		sql.deleteCharAt(sql.length() - 2);
		//complete sql query
		sql.append(") WITH ( OIDS = FALSE )");
		Log.logger.info("Create table: " + sql.toString());
		
		//create ownership sql
		StringBuilder sqlGrant = new StringBuilder();
		sqlGrant.append("ALTER TABLE " + tableName + " OWNER TO postgres");
		try {
			//drop the table
			PreparedStatement tableStatement = this.jdbcConnection.prepareStatement(sqlDrop.toString());
			boolean result = tableStatement.execute();
			
			//create the table
			tableStatement = this.jdbcConnection.prepareStatement(sql.toString());
			result = tableStatement.execute();
			
			//grant ownership
			tableStatement = this.jdbcConnection.prepareStatement(sqlGrant.toString());
			tableStatement.execute();

			//commit the transaction
			this.jdbcConnection.commit();
			tableStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			Log.logger.error("Unable to execute DDL create table statement");
		} catch (NullPointerException e) {
			Log.logger.error("Got null pointer exception running DDL with database");
		}
		
	}
	
	public void close() {
		try {
			this.jdbcConnection.close();
		} catch (SQLException e) {
			Log.logger.error("Unable to close database connection.");
		}
	}
}
