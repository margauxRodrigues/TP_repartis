import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Master {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ProcessBuilder pb = new ProcessBuilder("ls", "-al", "/tmp");
		try {
			Process p = pb.start();
			InputStream is = p.getInputStream();
			BufferedInputStream bis = new BufferedInputStream(is);
			InputStreamReader isr = new InputStreamReader(bis);
			BufferedReader br = new BufferedReader(isr);
			String line;
	        while ((line = br.readLine()) != null) { // while loop begins here
	           System.out.println(line);
	        } // end while 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
