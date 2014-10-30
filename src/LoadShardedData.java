/**
 *  This class attempts to shard the data and load it to two separate databsaes connected to switch
 */

import java.io.File;

import org.json.simple.JSONObject;

import resources.CreateSchema;
import resources.JDBCConnection;
import resources.Log;
import resources.ReadConfig;
import resources.ShardFile;

/**
 * @author neerajsharma
 *
 */
public class LoadShardedData {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//Read database config from config file
		ReadConfig config = new ReadConfig("config/server.conf");

		//read server config
		JSONObject serverConfig = config.get("server");
		JSONObject shardPgConfig = config.get("postgresqlShard");
		String dataPath = config.get(serverConfig, "dataPath");
		//Log.logger.info(config.get(serverConfig, "dataPath"));
		
		//create schema
		String createSchemaOption = config.get(serverConfig, "createSchema");
		if (createSchemaOption.equals("true")) {
			Log.logger.info("Creating schema from config file..");
			CreateSchema schemaCreation = new CreateSchema("config/schema.conf");
			schemaCreation.run();
			
			//create schema for shard too
//			if(serverConfig.get("shard").equals("true")) {
//				JDBCConnection shardJdbcConnection = new JDBCConnection(config.get(shardPgConfig, "url"),
//						config.get(shardPgConfig, "username"), config.get(shardPgConfig, "password"),
//						config.get(shardPgConfig, "autoCommit"));
//				CreateSchema shardSchema = new CreateSchema("config/schema.conf", shardJdbcConnection);
//				shardSchema.run();
//				shardJdbcConnection.close();
//			}
		}
		
		//use one instance of parsefile
		ShardFile dataFile = new ShardFile();
		
		File folder = new File(dataPath);
		File[] listOfFiles = folder.listFiles();

		long current = System.currentTimeMillis();
		for (File file : listOfFiles) {
		    if (file.isFile()) {
		    	Log.logger.info("Processing file: " + dataPath + "/" + file.getName());
		    	dataFile.shard(dataPath + "/" + file.getName());
		    }
		}
		long end = System.currentTimeMillis();
		Log.logger.info("Time taken for insertion: " + (end - current)/1000 + " seconds");
	}

}
