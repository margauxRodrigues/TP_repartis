import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Master {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/home/margaux/Documents/Cours/Systemes_repartis_big_data/TP/slave.jar");
		try {
			Process p = pb.start();
			InputStream is = p.getInputStream();
			InputStream es = p.getErrorStream();
			// Read output
			BufferedInputStream bis = new BufferedInputStream(is);
			InputStreamReader isr = new InputStreamReader(bis);
			BufferedReader br = new BufferedReader(isr);
			
			// Read errors
			BufferedInputStream bisError = new BufferedInputStream(es);
			InputStreamReader isrError = new InputStreamReader(bisError);
			BufferedReader brError = new BufferedReader(isrError);
			String errorLine;
			if ((errorLine = brError.readLine()) != null) {
				System.out.println(errorLine);
				while ((errorLine = brError.readLine()) != null) { // while loop begins here
					System.out.println(errorLine);
			        } // end while 
				}
			else {
				String line;
		        while ((line = br.readLine()) != null) { // while loop begins here
		           System.out.println(line);
		        } // end while 
			}
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
