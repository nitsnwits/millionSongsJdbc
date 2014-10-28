package resources;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Vector;

public class LargeReadDBReader extends Thread{

	static final int MAXQUEUE = 10000;
	JDBCConnection jdbcConnection = new JDBCConnection();
	Connection dbConnection = jdbcConnection.getConnection();
	static Long MAX = (long) 0;
	HashMap<String, String> reviews = new HashMap<String, String>();
	
	static Vector<HashMap<String, String>> messages = new Vector();

	@Override
	public void run() {
		try {
			while (true) {
				putMessage();
				//sleep(5000);
			}
		} catch (InterruptedException e) {
		}
	}

	private synchronized void putMessage() throws InterruptedException {

		MAX = (long) this.jdbcConnection.getCount("amazon_reviews");

		Long min = (long) 0;
		Long max = (long) 10000;

		while(true){

			while (messages.size() == MAXQUEUE) {
				wait();
			}

			if(max != MAX){
				ResultSet message = this.jdbcConnection.queryBetween("amazon_reviews", min, max);
				try {
					while (message.next()) {
						reviews.put("id", Integer.toString(message.getInt(1)));
						reviews.put("product_id", message.getString(2));
						reviews.put("title", message.getString(3));
						reviews.put("price", message.getString(4));
						reviews.put("user_id", message.getString(5));
						reviews.put("profile_name", message.getString(6));
						reviews.put("score", Double.toString(message.getDouble(7)));
						reviews.put("helpfulness", message.getString(8));
						reviews.put("review_time", Integer.toString(message.getInt(9)));
						reviews.put("review_summary", message.getString(10));
						reviews.put("review_text", message.getString(11));
						messages.addElement(reviews);
						reviews = new HashMap<String, String>();
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				messages = null;
			}
			
			min = max+1;
			max = max + 10000;
			
			if(max>MAX){
				max = MAX;
			}
			
			notify();
		}
		//Later, when the necessary event happens, the thread that is running it calls notify() from a block synchronized on the same object.
	}

	// Called by Consumer
	public synchronized Vector<HashMap<String, String>> getMessage() throws InterruptedException {
		notify();
		while (messages.size() == 0) {
			wait();//By executing wait() from a synchronized block, a thread gives up its hold on the lock and goes to sleep.
		}
		Vector<HashMap<String, String>> message = messages;
		messages=null;
		messages = new Vector<HashMap<String, String>>();
		if(messages==null){
			return null;
		}
		return message;
	}

}