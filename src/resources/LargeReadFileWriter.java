package resources;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
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

				Vector<ResultSet> message = dbReaderThread.getMessage();
				System.out.println("***************"+message.size());
				for (int i = 0; i < message.size(); i++) {
					ResultSet messageRow = (ResultSet) message.get(i);
					//System.out.println(dbTable.getTitle());
					try {
						writer.append("\n"+ messageRow.getNString(1));
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
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
