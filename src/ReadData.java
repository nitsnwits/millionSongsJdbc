import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.simple.JSONObject;

import resources.Log;
import resources.ReadConfig;
import resources.ReaderThread;

/**
 * Read data class, to read large influx
 * Create multiple threads and do concurrent reads to the table, selecting randomly the parameters
 */

/**
 * @author neerajsharma
 *
 */
public class ReadData {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//Read database config from config file
		ReadConfig config = new ReadConfig("config/server.conf");

		//read server config
		JSONObject serverConfig = config.get("server");
		int numThreads = Integer.parseInt(config.get(serverConfig, "concurrentReadThreads"));
		Log.logger.info("Simulating " + numThreads + " read clients to perform concurrent read.");
		
		//Get read frequency from server config
		int numReads = Integer.parseInt(config.get(serverConfig, "concurrentReadFrequency"));
		
		long current = System.currentTimeMillis();
		//create threads, each thread would make a read and log the request
		ExecutorService concurrentReadExecutor = Executors.newFixedThreadPool(numThreads);
		for (int i = 1; i <= numThreads; i++) {
			Runnable reader = new ReaderThread(Integer.toString(i), numReads);
			concurrentReadExecutor.execute(reader);
		}
		//finish the simulation
		concurrentReadExecutor.shutdown();
		while(!concurrentReadExecutor.isTerminated()) {
		}
		//make sure this is the last statement to be executed (vague though)
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Log.logger.info("Finished " + numThreads + " simulation threads successfully.");
		
		//finish program and calculate performance
		long end = System.currentTimeMillis();
		//total queries: numReads into numThreads , time is calculated from here
		int totalQueries = numReads * numThreads;
		int timeTakenInSeconds = (int)(end - current)/1000;
		int queryPerSecond = totalQueries/timeTakenInSeconds;
		Log.logger.info("Performance in queries per second: " + queryPerSecond);	
	}

}
