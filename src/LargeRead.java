import resources.LargeReadDBReader;
import resources.LargeReadFileWriter;


public class LargeRead {

	public static void main(String[] args) {
		LargeReadDBReader dbReader =  new LargeReadDBReader();
		dbReader.start();
		new LargeReadFileWriter(dbReader).start();
	}
}
