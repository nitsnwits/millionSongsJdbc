import resources.LargeReadDBReader;
import resources.LargeReadFileWriter;
import resources.Log;


public class LargeRead {

	public static void main(String[] args) {
		//finish program and calculate performance
		long current = System.currentTimeMillis();
		LargeReadDBReader dbReader =  new LargeReadDBReader();
		dbReader.start();
		new LargeReadFileWriter(dbReader).start();
		long end = System.currentTimeMillis();
		Log.logger.info("Time taken for dump: " + (end - current)/1000 + " seconds");
	}
}
