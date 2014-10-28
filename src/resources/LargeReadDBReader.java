package resources;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.Vector;

import javax.management.Query;

public class LargeReadDBReader extends Thread{

	static final int MAXQUEUE = 10000;
	JDBCConnection jdbcConnection = new JDBCConnection();
	Connection dbConnection = jdbcConnection.getConnection();
	static Long MAX = (long) 0;
	
	static Vector<ResultSet> messages = new Vector();

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
				messages.addElement(message);
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
	public synchronized Vector<ResultSet> getMessage() throws InterruptedException {
		notify();
		while (messages.size() == 0) {
			wait();//By executing wait() from a synchronized block, a thread gives up its hold on the lock and goes to sleep.
		}
		Vector<ResultSet> message = messages;
		messages=null;
		messages = new Vector<ResultSet>();
		if(messages==null){
			return null;
		}
		return message;
	}

}