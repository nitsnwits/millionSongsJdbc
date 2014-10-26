package resources;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

public class ReaderThread implements Runnable{

	String threadId;
	Random randomNumberGenerator;
	JDBCConnection jdbcConnection;
	Connection dbConnection;
	int maxCount = 0;
	int frequency = 0;

	public ReaderThread(String threadId, int frequency) {
		this.threadId = threadId;
		this.randomNumberGenerator = new Random();
		this.jdbcConnection = new JDBCConnection();
		this.dbConnection = this.jdbcConnection.getConnection();
		//get the maximum count from the database
		this.maxCount = this.jdbcConnection.getCount("amazon_reviews");
		this.frequency = frequency;
	}

	private void processCommand() {
		try {
			//stub the thread here to catch interrupted exception
			Thread.sleep(0);
			
			for(int i=1; i<=frequency; i++) {
			    //a range of id's present in the database, should get from count of database
				int randomInt = randomNumberGenerator.nextInt(maxCount) + 1; //to account for zero limit
				//fire a query and log it
				int queryResult = this.jdbcConnection.selectById(randomInt, "amazon_reviews");
				if(i % 5000 == 0) {
					Log.logger.info("Thread Id: " + threadId + " Query Result ID: " + queryResult);
				}
			}
			this.jdbcConnection.close();
		   
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		processCommand();
	}

}
