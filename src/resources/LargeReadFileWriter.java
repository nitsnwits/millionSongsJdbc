package resources;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Vector;

public class LargeReadFileWriter extends Thread{

	LargeReadDBReader dbReaderThread;

	public LargeReadFileWriter(LargeReadDBReader dbReaderThread) {
		// TODO Auto-generated constructor stub
		this.dbReaderThread = dbReaderThread;
	}

	@Override
	public void run() {

		try {
			PrintWriter writer= new PrintWriter("amazon.txt", "UTF-8");
			while (true) {

				Vector<HashMap<String, String>> message = dbReaderThread.getMessage();
				//Log.logger.info("Processing received message size: " + message.size());
				for (int i = 0; i < message.size(); i++) {
					HashMap<String, String> row = message.get(i);
					
					StringBuilder sb = new StringBuilder();
					sb.append(row.get("id") + ",");
					sb.append(row.get("product_id") + ",");
					sb.append(row.get("title") + ",");
					sb.append(row.get("price") + ",");
					sb.append(row.get("user_id") + ",");
					sb.append(row.get("profile_name") + ",");
					sb.append(row.get("score") + ",");
					sb.append(row.get("helpfulness") + ",");
					sb.append(row.get("review_time") + ",");
					sb.append(row.get("review_summary") + ",");
					sb.append(row.get("review_text") + "\n");
					writer.append(sb);
				}

			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
