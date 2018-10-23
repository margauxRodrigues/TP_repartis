import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

public class Master {

	public static void main(String[] args) throws InterruptedException, IOException {
		// Copy split files on all machines
		String machinesFile = "../machine_list.txt";
		String outputDirectory = "/tmp/mrodrigues/splits";
		String inputDirectory = "/tmp/mrodrigues/splits";
		ArrayList <String> filesToCopy = new ArrayList<String>();
		filesToCopy.add("S0.txt");
		filesToCopy.add("S1.txt");
		filesToCopy.add("S2.txt");
		
		copyFilesToMachines(inputDirectory, outputDirectory, filesToCopy, machinesFile);
		
		// Ici le directory splits est déjà fait et toutes les machines ont les Sx.
		
		// Lancer le slave sur les machines, un map par machine
		ArrayList <Process> runningProcess = new ArrayList<Process>();
		ArrayList <String> machinesList = readTxt(machinesFile);
		HashMap <String, String> machineFileMap = new HashMap<String, String>();
		// Ne fonctionne que si nbre de fichiers <= nombre de machines
		for (int s=0; s<filesToCopy.size(); s++) {
			// Slaves lancés
			ProcessBuilder pb = new ProcessBuilder("ssh", "mrodrigues@" + machinesList.get(s), "java", "-jar", "/tmp/mrodrigues/slave.jar",
					"0", "/tmp/mrodrigues/splits/" + filesToCopy.get(s));
			Process p = pb.start();
			runningProcess.add(p);
		}
		HashMap< String, ArrayList<String>> keyMachinesDict = new HashMap< String, ArrayList<String> >();
		for (int p=0; p<runningProcess.size(); p++) {
			// Attendre tous les process et récupérer le dictionnaire
			String[] a  = readProcessOutput(runningProcess.get(p)).split("\n"); // liste des clés
			System.out.println(a[0]);
			machineFileMap.put("UM" + p + ".txt", machinesList.get(p));
			
			for (int i=0; i<a.length; i++) {
				if (keyMachinesDict.containsKey(a[i])) {
					// update liste de machines
					keyMachinesDict.get(a[i]).add(machinesList.get(p));
				}
				else {
					// ajouter la clé et intialiser la liste de machines
					ArrayList<String> listMachines = new ArrayList<String>();
					listMachines.add(machinesList.get(p));
					keyMachinesDict.put(a[i], listMachines);
				}
			}
				
		}
		
		// a modifier pour qu'il affiche clé - liste de machines
		printDict(machineFileMap);
		printDict(keyMachinesDict);
	}
	
	
	// ---------------- READ PROCESS OUTPUT -----------------------------------
	public static String readProcessOutput(Process p) throws IOException {
		InputStream is = p.getInputStream();
		InputStream es = p.getErrorStream();
		// Read output
		BufferedInputStream bis = new BufferedInputStream(is);
		InputStreamReader isr = new InputStreamReader(bis);
		BufferedReader br = new BufferedReader(isr);
		String output;
		// Read errors
		BufferedInputStream bisError = new BufferedInputStream(es);
		InputStreamReader isrError = new InputStreamReader(bisError);
		BufferedReader brError = new BufferedReader(isrError);
		String errorLine;
		if ((errorLine = brError.readLine()) != null) {
			System.out.println(errorLine);
			output = errorLine;
			while ((errorLine = brError.readLine()) != null) { // while loop begins here
				output += errorLine+"\n";
		        } // end while 
			}
		else {
			String line;
			output = "";
	        while ((line = br.readLine()) != null) { // while loop begins here
	           output += line+"\n";
	        } // end while 
		}	
		return output;
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
		
		// Copy files
		for (int i=0; i<machinesList.size(); i++) {
			for (int j=0; j<filename.size();j++) {
				ProcessBuilder pb = new ProcessBuilder("scp", 
						inputDir + "/" + filename.get(j),
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
	
	public static void printDict (HashMap dict) {
		for (Object key : dict.keySet()) {
			System.out.println(key + " : " + dict.get(key));
		}
	}


}
