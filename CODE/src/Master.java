import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class Master {

	public static void main(String[] args) throws InterruptedException, IOException {
		// Launch slave.jar on all machines
		/*String machineName = "mrodrigues@C133-04";
		ProcessBuilder pb = new ProcessBuilder("ssh",machineName,"java", "-jar", "/tmp/mrodrigues/slave.jar");
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
		}*/
		
		// Copy split files on all machines
		String machinesFile = "../machine_list_test.txt";
		String outputDirectory = "/tmp/mrodrigues/splits";
		String inputDirectory = "../";
		ArrayList <String> filesToCopy = new ArrayList<String>();
		filesToCopy.add("S0.txt");
		filesToCopy.add("S1.txt");
		filesToCopy.add("S2.txt");
		
		copyFilesToMachines(inputDirectory, outputDirectory, filesToCopy, machinesFile);
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

	public static void copyFilesToMachines(String inputDir, String outputDir, ArrayList<String> filename, String machinesListDir) throws IOException, InterruptedException {
		ArrayList<String>  machinesList = readTxt(machinesListDir);
		ArrayList<Process> runningProcess = new ArrayList<Process>();
		
		// Create /tmp/mrodrigues directory if doesn't exist on all machines
		for (int i=0; i<machinesList.size(); i++) {
			ProcessBuilder pb = new ProcessBuilder("ssh", "mrodrigues@" + machinesList.get(i), "mkdir", "-p", outputDir);
			Process p = pb.start();
			runningProcess.add(p);
		}
		for (int i=0; i<runningProcess.size(); i++) {
			runningProcess.get(i).waitFor();
		}
		
		// Copy jar file to all machines
		for (int i=0; i<machinesList.size(); i++) {
			for (int j=0; j<filename.size();j++) {
				ProcessBuilder pb = new ProcessBuilder("scp", 
						inputDir + filename.get(i),
						"mrodrigues@" + machinesList.get(i) + ":" + outputDir + "/" + filename.get(j));
				Process p = pb.start();
				runningProcess.add(p);
			}
		}
		for (int i=0; i<runningProcess.size(); i++) {
			readProcessOutput(runningProcess.get(i));
		}
	}
	// -----------------------------------
	
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

}
