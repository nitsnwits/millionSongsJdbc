/*
 * Class to parse file and shard it multiple servers
 */

package resources;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.json.simple.JSONObject;

/**
 * @author neerajsharma
 *
 */

public class ShardFile {

	public void shard(String filePath) {
		InputStream fileStream;
		String line = null;
		int rowSeparator = 0;
		int rowNumber = 1;
		int rowId = 1;
		
		//use sharded jdbc connections
		//Read database config from config file
		ReadConfig config = new ReadConfig("config/server.conf");

		//read server and postgresql config
		JSONObject mainPgConfig = config.get("postgresql");
		JSONObject shardPgConfig = config.get("postgresqlShard");
		JSONObject serverConfig = config.get("server");
		
		JDBCConnection mainJdbcConnection = new JDBCConnection(config.get(mainPgConfig, "url"), 
				config.get(mainPgConfig, "username"), config.get(mainPgConfig, "password"), 
				config.get(mainPgConfig, "autoCommit"));
		Connection dbConnection = mainJdbcConnection.getConnection();
		
		JDBCConnection shardJdbcConnection = new JDBCConnection(config.get(shardPgConfig, "url"),
				config.get(shardPgConfig, "username"), config.get(shardPgConfig, "password"),
				config.get(shardPgConfig, "autoCommit"));
		Connection shardDbConnection = shardJdbcConnection.getConnection();
		
		PreparedStatement schema3ps1main = mainJdbcConnection.prepareStatement(4);
		PreparedStatement schema3ps2main = mainJdbcConnection.prepareStatement(5);		

		PreparedStatement schema3ps1shard = shardJdbcConnection.prepareStatement(4);
		PreparedStatement schema3ps2shard = shardJdbcConnection.prepareStatement(5);
		
		try {
			//read the gzip file directly, to avoid decompression
			fileStream = new FileInputStream(filePath);
			InputStream gzipStream = new GZIPInputStream(fileStream);
			Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
			BufferedReader buffered = new BufferedReader(decoder);

			HashMap<String, String> reviews = new HashMap<String, String>();

			//read the file line by line
			while ((line = buffered.readLine()) != null) {
				//blank line is a row separator
				if (line.length()!=0) {

					if (line.startsWith("product")){
						//split the row on : delimiter
						String[] string = line.split(":");
						String[] key = string[0].split("/");
						
						//put the key as hash key, and value as hash value
						if(string[1].contains("'")) {
							string[1] = string[1].replace("'", "''");
						}
						reviews.put(key[1], string[1].trim());
						
					} else if(line.startsWith("review")){
						//split the row on : delimiter
						String[] string = line.split(":");
						String[] key = string[0].split("/");
						
						//put the key as hash key, and value as hash value
						if(string[1].contains("'")) {
							string[1] = string[1].replace("'", "''");
						}
						reviews.put(key[1], string[1].trim());
						
					}

				} else {
					//check if reviews contains the valid columns, else create default values
					if(!reviews.containsKey("text")) {
						reviews.put("text", "");
					}		
					
					//update the code to handle batches
					reviews.put("id", ""+rowId);
					
					//shard the data and prepare batches
					if(rowNumber % 2 == 0) {
						//go to main shard
						mainJdbcConnection.insertSchema3(reviews, schema3ps1main, schema3ps2main);
					} else {
						//go to second shard
						shardJdbcConnection.insertSchema3(reviews, schema3ps1shard, schema3ps2shard);
					}
					if(rowNumber % 100000 == 0) {
						mainJdbcConnection.executeBatch(schema3ps1main, schema3ps2main);
						shardJdbcConnection.executeBatch(schema3ps1shard, schema3ps2shard);
						mainJdbcConnection.commit();
						shardJdbcConnection.commit();
						schema3ps1main = mainJdbcConnection.prepareStatement(4);
						schema3ps2main = mainJdbcConnection.prepareStatement(5);		

						schema3ps1shard = shardJdbcConnection.prepareStatement(4);
						schema3ps2shard = shardJdbcConnection.prepareStatement(5);
						
						Log.logger.info("Commit rows per shard: " + rowNumber);
					}					
					rowNumber++;
					rowId++;
				}
			}
			//commit the final rows left in the batch, if less than the batch size
			mainJdbcConnection.executeBatch(schema3ps1main, schema3ps2main);
			shardJdbcConnection.executeBatch(schema3ps1shard, schema3ps2shard);
			mainJdbcConnection.commit();
			shardJdbcConnection.commit();
			schema3ps1main = mainJdbcConnection.prepareStatement(4);
			schema3ps2main = mainJdbcConnection.prepareStatement(5);		

			schema3ps1shard = shardJdbcConnection.prepareStatement(4);
			schema3ps2shard = shardJdbcConnection.prepareStatement(5);
			Log.logger.info("Final Commit rows: " + rowNumber);
			
		} catch (FileNotFoundException e) {
			Log.logger.error("File not found: ", filePath);
		} catch (IOException e) {
			Log.logger.error("Unable to read file: ", filePath);
		} catch (NullPointerException e) {
			Log.logger.error("Null pointer procesing file");
			e.printStackTrace();
		} finally {
			Log.logger.info("Completed insertion: " + rowNumber);
			mainJdbcConnection.commit();
			mainJdbcConnection.close();
			shardJdbcConnection.commit();
			shardJdbcConnection.close();
		}

	}

}
