package resources;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Set;

import org.json.simple.JSONObject;

/**
 * 
 */

/**
 * @author neerajsharma
 *
 */
public class CreateSchema {
	private JDBCConnection dbConnection;
	private JSONObject schema;
	private ReadConfig schemaConfig;
	private int numOfTables;
	private Set<String> schemaKeys;
	
	/**
	 * Create tables based on schema config
	 */
	public CreateSchema(String filePath) {
		//file path is schema.conf file path
		
		//Read schema config from schema config file
		this.schemaConfig = new ReadConfig(filePath);
		this.schema = schemaConfig.get();
		
		//number of tables
		this.schemaKeys = schema.keySet();
		this.numOfTables = schemaKeys.size();
		
		//Having only one database connection for schema config
		//get database connection
		this.dbConnection = new JDBCConnection();
	}
	
	public void run() {
		HashMap<String, String> tableHash = new HashMap<String, String>();
		//create table for each schema give in conf file
		for (String key: schemaKeys) {
			//get the table schema for this key
			tableHash = getTableSchema(key);
			//create schema for this table
			Log.logger.info("Creating table: " + key);
			Log.logger.info("table details: " + tableHash);
			dbConnection.createTable(tableHash, key);
			//reset hash
			tableHash = new HashMap<String, String>();
		}		
	}
	
	public HashMap<String, String> getTableSchema(String tableName) {
		HashMap<String, String> tableHash = new HashMap<String, String>();
		JSONObject table = schemaConfig.get(tableName);
		Set<String> tableKeys = table.keySet();
		//one tablehash has one table columns
		for (String tableKey: tableKeys) {
			tableHash.put(tableKey, schemaConfig.get(table, tableKey));
		}
		return tableHash;
	}

}
