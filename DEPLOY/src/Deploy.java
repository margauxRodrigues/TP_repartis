import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class Deploy {

	public static void main(String[] args) throws IOException {
		// Tester la connection SSH avec les machines dont les noms sont enregistrés dans un fichier txt
		String filename = "../machine_list.txt";
		ArrayList<String>  machinesList = readTxt(filename);
		ArrayList<Process> runningProcess = new ArrayList<Process>();
		// Launch connection for all machines in list
		for (int i=0; i<machinesList.size(); i++) {
			ProcessBuilder pb = new ProcessBuilder("ssh", "mrodrigues@" + machinesList.get(i), "hostname");
			Process p = pb.start();
			runningProcess.add(p);
		}
		for (int i=0; i<runningProcess.size(); i++) {
			readProcessOutput(runningProcess.get(i));
		}
	}
	
	public static ArrayList<String> readTxt (String filename) throws IOException {
		ArrayList<String> text = new ArrayList<String>();
		File f = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(f));
		String readLine;
		while ((readLine = br.readLine()) != null){
			text.add(readLine);
		}
		br.close();
		return text;
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

