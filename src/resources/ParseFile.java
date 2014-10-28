/*
 * Class to parse one GZ file without unzipping
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

/**
 * @author neerajsharma
 *
 */

public class ParseFile {

	public void parse(String filePath) {
		InputStream fileStream;
		String line = null;
		int rowSeparator = 0;
		int rowNumber = 1;
		int rowId = 1;
		JDBCConnection jdbcConnection = new JDBCConnection();
		Connection dbConnection = jdbcConnection.getConnection();
		//PreparedStatement batchPreparedStatement = jdbcConnection.prepareStatement();
		PreparedStatement schema2ps1 = jdbcConnection.prepareStatement(1);
		PreparedStatement schema2ps2 = jdbcConnection.prepareStatement(2);
		PreparedStatement schema2ps3 = jdbcConnection.prepareStatement(3);

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
					//TODO: this is just a sanity check
//					if(!reviews.containsKey("product_id")) {
//						reviews.put("product_id", "defaultProductId")
//					}
					if(!reviews.containsKey("text")) {
						reviews.put("text", "");
					}
					
					//To use jdbc individual insert
					/*
					jdbcConnection.insert(reviews, "amazon_reviews");
					if (rowNumber % 100000 == 0) {
						Log.logger.info("Commited rows: " + rowNumber);
						jdbcConnection.commit();
					}
					reviews = new HashMap<String, String>();
					rowNumber++;*/			
					
					//update the code to handle batches
					reviews.put("id", ""+rowId);
//					jdbcConnection.addToBatch(reviews, batchPreparedStatement);
//					if(rowNumber % 100000 == 0) {
//						jdbcConnection.executeBatch(batchPreparedStatement);
//						jdbcConnection.commit();
//						batchPreparedStatement = jdbcConnection.prepareStatement();
//						Log.logger.info("Commit rows: " + rowNumber);
//					}
					
					//update the code to handle schema 2
					jdbcConnection.insertSchema2(reviews, schema2ps1, schema2ps2, schema2ps3);
					if(rowNumber % 1000 == 0) {
						jdbcConnection.executeBatch(schema2ps1, schema2ps2, schema2ps3);
						jdbcConnection.commit();
						schema2ps1 = jdbcConnection.prepareStatement(1);
						schema2ps2 = jdbcConnection.prepareStatement(2);
						schema2ps3 = jdbcConnection.prepareStatement(3);
					}
					rowNumber++;
					rowId++;
				}
			}
			//commit the final rows left in the batch, if less than the batch size
			jdbcConnection.executeBatch(schema2ps1, schema2ps2, schema2ps3);
			jdbcConnection.commit();
			schema2ps1 = jdbcConnection.prepareStatement(1);
			schema2ps2 = jdbcConnection.prepareStatement(2);
			schema2ps3 = jdbcConnection.prepareStatement(3);
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
			jdbcConnection.commit();
			jdbcConnection.close();
		}

	}

}
