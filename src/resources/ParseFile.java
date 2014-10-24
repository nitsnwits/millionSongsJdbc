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
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import resources.CreateSchema;

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
		JDBCConnection jdbcConnection = new JDBCConnection();
		Connection dbConnection = jdbcConnection.getConnection();
		

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
					jdbcConnection.insert(reviews, "amazon_reviews");
					if (rowNumber % 100000 == 0) {
						Log.logger.info("Commited rows: " + rowNumber);
						jdbcConnection.commit();
					}
					reviews = new HashMap<String, String>();
					rowNumber++;
				}
			}
			
		} catch (FileNotFoundException e) {
			Log.logger.error("File not found: ", filePath);
		} catch (IOException e) {
			Log.logger.error("Unable to read file: ", filePath);
		} finally {
			Log.logger.info("Completed insertion: " + rowNumber);
			jdbcConnection.commit();
			jdbcConnection.close();
		}

	}

}
