import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

public class Master {

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/home/margaux/Documents/Cours/Systemes_repartis_big_data/TP/slave.jar");
		pb.inheritIO();
		try {
			Process p = pb.start();
			// Timeout de deux secondes
			boolean b = p.waitFor(15, TimeUnit.SECONDS);
			if (!b) {
				p.destroy();
				System.out.println("Timeout ! ");
			}
			else {
				readProcessOutput(p);
			}
			
			
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void readProcessOutput(Process p) throws IOException {
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

}
