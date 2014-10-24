/**
 * 
 */

import java.io.File;
import java.util.HashMap;
import java.util.Set;

import org.json.simple.JSONObject;

import resources.CreateSchema;
import resources.Log;
import resources.ParseFile;
import resources.ReadConfig;

/**
 * @author neerajsharma
 *
 */
public class LoadData {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//Read database config from config file
		ReadConfig config = new ReadConfig("config/server.conf");

		//read server config
		JSONObject serverConfig = config.get("server");
		String dataPath = config.get(serverConfig, "dataPath");
		//Log.logger.info(config.get(serverConfig, "dataPath"));
		
		//create schema
		String createSchemaOption = config.get(serverConfig, "createSchema");
		if (createSchemaOption.equals("true")) {
			Log.logger.info("Creating schema from config file..");
			CreateSchema schemaCreation = new CreateSchema("config/schema.conf");
			schemaCreation.run();
		}
		
		//use one instance of parsefile
		ParseFile dataFile = new ParseFile();
		
		File folder = new File(dataPath);
		File[] listOfFiles = folder.listFiles();

		long current = System.currentTimeMillis();
		for (File file : listOfFiles) {
		    if (file.isFile()) {
		    	Log.logger.info("Processing file: " + dataPath + "/" + file.getName());
		    	dataFile.parse(dataPath + "/" + file.getName());
		    }
		}
		long end = System.currentTimeMillis();
		Log.logger.info("Time taken for insertion: " + (end - current)/1000 + " seconds");
	}

}
