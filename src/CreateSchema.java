import java.sql.Connection;
import java.util.HashMap;
import java.util.Set;

import org.json.simple.JSONObject;

import resources.JDBCConnection;
import resources.Log;
import resources.ReadConfig;

/**
 * 
 */

/**
 * @author neerajsharma
 *
 */
public class CreateSchema {
	private JDBCConnection dbConnection;
	
	/**
	 * Create tables based on schema config
	 */
	public CreateSchema() {
		// TODO Auto-generated constructor stub
	}
	
	public void run(String filePath) {
		//file path is schema.conf file path
		
		//Read schema config from schema config file
		ReadConfig schemaConfig = new ReadConfig(filePath);
		JSONObject schema = schemaConfig.get();
		
		//get database connection
		JDBCConnection jdbcConnection = new JDBCConnection();
		Connection dbConnection = jdbcConnection.getConnection();
		
		//set internal values
		this.dbConnection = jdbcConnection;
		
		HashMap<String, String> tableHash = new HashMap<String, String>();
		Set<String> schemaKeys = schema.keySet();
		for (String key: schemaKeys) {
			//create table for each schema give in conf file
			JSONObject table = schemaConfig.get(key);
			Set<String> tableKeys = table.keySet();
			//one tablehash has one table columns
			for (String tableKey: tableKeys) {
				tableHash.put(tableKey, schemaConfig.get(table, tableKey));
			}
			//create schema for this table
			Log.logger.info("Creating table: " + key);
			Log.logger.info("table details: " + tableHash);
			jdbcConnection.createTable(tableHash, key);
			//reset hash
			tableHash = new HashMap<String, String>();
		}		
	}
	
	public String[] getTableSchema() {
		//TODO: Use tableSchema to insert directly to tables than defining columns
		String[] tableSchema = new String[10];
		return tableSchema;
	}

}
