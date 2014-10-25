/**
 * Provides one JDBC connection to the database
 */
package resources;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import resources.GenerateUUID;

import org.json.simple.JSONObject;

/**
 * @author neerajsharma, sameersave
 *
 */
public class JDBCConnection {
	private Connection jdbcConnection;
	private String pgsqlUrl, pgsqlUsername, pgsqlPassword;
	private boolean autoCommit = true;
	private String sqlInsert = "INSERT INTO amazon_reviews "
			+ "(id, product_id, title, price, "
			+ "user_id, profile_name, helpfulness, score, review_time, review_summary, review_text ) VALUES "
			+ "(?,?,?,?,?,?,?,?,?,?,?)";
	//private PreparedStatement preparedStatement;
	//Have schema of each table available
	//private CreateSchema schema;
	
	public JDBCConnection() {

		//Read database config from config file TODO: change interface to read from param
		ReadConfig config = new ReadConfig("config/server.conf");
		
		//this.schema = new CreateSchema("config/schema.conf");

		//read server and postgresql config
		JSONObject postgreSqlConfig = config.get("postgresql");
		JSONObject serverConfig = config.get("server");
		
		//set JSON keys that are being read from server.conf
		this.pgsqlUrl = config.get(postgreSqlConfig, "url");
		this.pgsqlUsername = config.get(postgreSqlConfig, "username");
		this.pgsqlPassword = config.get(postgreSqlConfig, "password");
		
		//somehow, json object is not able to read boolean :(
		String pgsqlAutoCommit = config.get(postgreSqlConfig, "autoCommit");
		if (pgsqlAutoCommit.equalsIgnoreCase("false")) {
			this.autoCommit = false;
		} else if (pgsqlAutoCommit.equalsIgnoreCase("true")) {
			this.autoCommit = true;
		} else {
			Log.logger.warn("No autocommit configuration set, using default: true");
		}
		
		//register postgresql drivers
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			Log.logger.error("Postgresql driver not loaded.");
		}
		
		//now, create jdbc connection
		try {
			this.jdbcConnection = DriverManager.getConnection(pgsqlUrl, pgsqlUsername, pgsqlPassword);
			this.jdbcConnection.setAutoCommit(autoCommit);
			
			//create instance's prepared statement for batch processing
			//this.jdbcConnection.prepareStatement(this.sqlInsert);
		} catch (SQLException e) {
			Log.logger.error("Unable to load connection to database.");
		}		
	}
	
	public Connection getConnection() {
		return this.jdbcConnection;
	}
	
//	public PreparedStatement getPreparedStatement() {
//		return this.preparedStatement;
//	}
	
	public void commit() {
		try {
			this.jdbcConnection.commit();
		} catch (SQLException e) {
			Log.logger.error("Unable to commit to database.");
		}
	}
	
	public void insert(HashMap<String, String> row, String tableName) {
		//Set insert in a table		
		try {
//			StringBuilder sqlInsert = new StringBuilder();
//			sqlInsert.append("INSERT INTO amazon_reviews (");
//			sqlInsert.append("id, product_id, title, price, ");
//			sqlInsert.append("user_id, profile_name, helpfulness, score, review_time, review_summary, review_text ) values (");
			
//			String id = "'" + GenerateUUID.get().toString() + "', ";
//			String product_id = "'" + row.get("productId") + "', ";
//			String title = "'" + row.get("title") + "', ";
//			String price = "'" + row.get("price") + "', ";
//			String user_id = "'" + row.get("userId") + "', ";
//			String profile_name = "'" + row.get("profileName") + "', ";
//			String helpfulness = "'" + row.get("helpfulness") + "', ";
//			String review_summary = "'" + row.get("summary") + "', ";
//			String review_text = "'" + row.get("text") + "')";
			
//			sqlInsert.append(id + product_id + title + price + user_id + profile_name + helpfulness + row.get("score") + ", " + row.get("time") + ", " + review_summary + review_text);;

			//Log.logger.info("insert: " + sqlInsert.toString());
			
			//Set the prepared statement values
			PreparedStatement preparedStatement = this.jdbcConnection.prepareStatement(sqlInsert);
			preparedStatement.setString(1, GenerateUUID.get().toString());
			preparedStatement.setString(2, row.get("productId"));
			preparedStatement.setString(3, row.get("title"));
			preparedStatement.setString(4, row.get("price"));
			preparedStatement.setString(5, row.get("userId"));
			preparedStatement.setString(6, row.get("profileName"));
			preparedStatement.setString(7, row.get("helpfulness"));
			preparedStatement.setDouble(8, Double.parseDouble(row.get("score")));
			preparedStatement.setInt(9, Integer.parseInt(row.get("time")));
			preparedStatement.setString(10, row.get("summary"));
			preparedStatement.setString(11, row.get("text"));

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
		//Log.logger.info("Create table: " + sql.toString());
		
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
	
	public PreparedStatement prepareStatement() {
		try {
			return this.jdbcConnection.prepareStatement(sqlInsert);
		} catch (SQLException e) {
			Log.logger.warn("Unable to create prepared statement");
			e.printStackTrace();
			return null;
		}
	}
	
	public void addToBatch(HashMap<String, String> row, PreparedStatement ps) {
		//Set the prepared statement values
		
		try {
			ps.setString(1, GenerateUUID.get().toString());
			ps.setString(2, row.get("productId"));
			ps.setString(3, row.get("title"));
			ps.setString(4, row.get("price"));
			ps.setString(5, row.get("userId"));
			ps.setString(6, row.get("profileName"));
			ps.setString(7, row.get("helpfulness"));
			ps.setDouble(8, Double.parseDouble(row.get("score")));
			ps.setInt(9, Integer.parseInt(row.get("time")));
			ps.setString(10, row.get("summary"));
			ps.setString(11, row.get("text"));
			ps.addBatch();
		} catch (NumberFormatException e) {
			e.printStackTrace();
			Log.logger.warn("Number format exception in adding batches");
		} catch (SQLException e) {
			e.printStackTrace();
			Log.logger.warn("SQL Exception in adding batches");
		}
	}
	
	public void executeBatch(PreparedStatement ps) {
		try {
			//execute the batch and reset prepared statement
			ps.executeBatch();
			//reset ps
			ps.close();
			//this.preparedStatement = this.jdbcConnection.prepareStatement(sqlInsert);
		} catch (SQLException e) {
			e.printStackTrace();
			Log.logger.error("Unable to execute batch, rolling back");
			
			//try to rollback
			try {
				this.jdbcConnection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
				Log.logger.error("Unable to rollback the batch, inconsistency may arise");
			}
		}
	}
}
