import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Master {

	public static void main(String[] args) throws InterruptedException, IOException {
		// Copy split files on all machines
		String machinesFile = "/home/margaux/Documents/Cours/Systemes_repartis_big_data/TP/machine_list.txt";
		String outputDirectory = "/tmp/mrodrigues/splits";
		String inputDirectory = "/home/margaux/Documents/Cours/Systemes_repartis_big_data/TP/Splits";
		ArrayList <String> filesToCopy = new ArrayList<String>();
		File directory = new File(inputDirectory);
		File[] paths = directory.listFiles();
		System.out.println("" + paths[0]);
		for (int f=0; f<paths.length; f++) {
			filesToCopy.add(paths[f].getName());
		}
		ArrayList <Process> runningProcess = new ArrayList<Process>();
		ArrayList <String> machinesList = testMachines(machinesFile);
		HashMap <String, String> machineFileMap = new HashMap<String, String>();
		// Ne fonctionne que si nbre de fichiers <= nombre de machines
		int machinesNumber = machinesList.size();
		int machine_ind = 0;
		for (int s=0; s<filesToCopy.size(); s++) {
			// Slaves lancés
			ProcessBuilder pb_dir = new ProcessBuilder("ssh", "mrodrigues@" + machinesList.get(machine_ind), "mkdir", "-p", outputDirectory);
			Process p_dir = pb_dir.start();
			p_dir.waitFor();
			
			ProcessBuilder pbCopy = new ProcessBuilder("scp", inputDirectory + "/" + filesToCopy.get(s), "mrodrigues@" + machinesList.get(machine_ind) + ":/tmp/mrodrigues/splits/");
			Process pCopy = pbCopy.start();
			pCopy.waitFor();
			ProcessBuilder pb = new ProcessBuilder("ssh", "mrodrigues@" + machinesList.get(machine_ind), "java", "-jar", "/tmp/mrodrigues/slave.jar",
					"0", "/tmp/mrodrigues/splits/" + filesToCopy.get(s));
			Process p = pb.start();
			runningProcess.add(p);
			if (machine_ind == (machinesNumber - 1)) {
				machine_ind = 0;
			}
			else {
				machine_ind ++;
			}
		}
		
		HashMap< String, ArrayList<String>> keyWordUm = new HashMap< String, ArrayList<String> >();
		for (int p=0; p<runningProcess.size(); p++) {
			// Attendre tous les process et récupérer le dictionnaire
			String[] a  = readProcessOutput(runningProcess.get(p)).split("\n"); // liste des clés
			machineFileMap.put("UM" + p + ".txt", machinesList.get(p));
			
			for (int i=0; i<a.length; i++) {
				if (keyWordUm.containsKey(a[i])) {
					// update liste de machines
					for (int j=0; j<runningProcess.size(); j++) {
						runningProcess.get(j).waitFor();
					}
					keyWordUm.get(a[i]).add("UM" + p + ".txt");
				}
				else {
					// ajouter la clé et intialiser la liste de machines
					ArrayList<String> listMachines = new ArrayList<String>();
					listMachines.add("UM" + p + ".txt");
					keyWordUm.put(a[i], listMachines);
				}
			}
		}
		printDict(machineFileMap);
		printDict(keyWordUm);
		System.out.println("Phase de map terminée");
		
		// --------------------------------- PREPARATION SHUFFLE + SHUFFLE -----------------
		
		ArrayList <Process> runningProcessShufflePrep = new ArrayList<Process>();
		int countKeys = 1;
		for (String word : keyWordUm.keySet() )
		{
			String machineToCopy = machineFileMap.get(keyWordUm.get(word).get(0));
			List<String> arguments = new ArrayList <String>();
			arguments.add("ssh");
			arguments.add("mrodrigues@" + machineToCopy);
			arguments.add("java");
			arguments.add("-jar");
			arguments.add("/tmp/mrodrigues/slave.jar");
			arguments.add("1");
			arguments.add(word);
			arguments.add("/tmp/mrodrigues/maps/SM" + countKeys + ".txt");
			arguments.add("/tmp/mrodrigues/maps/" + keyWordUm.get(word).get(0));
			for (int j=1; j<keyWordUm.get(word).size(); j++)
			{
				// Get name of UM + location
				String UMname = keyWordUm.get(word).get(j);
				String location = machineFileMap.get(UMname);
				// Copy UM files to first machine
				ProcessBuilder pb = new ProcessBuilder("scp", 
						"mrodrigues@" + location + ":/tmp/mrodrigues/maps/" + UMname,
						"mrodrigues@" + machineToCopy + ":/tmp/mrodrigues/maps/");
				Process p = pb.start();
				p.waitFor();
				System.out.println("Copy file " + UMname + " to " + machineToCopy + " for " + word);
				arguments.add("/tmp/mrodrigues/maps/" + keyWordUm.get(word).get(j));
			}
			ProcessBuilder pb = new ProcessBuilder(arguments);
			Process p = pb.start();
			runningProcessShufflePrep.add(p);
			countKeys ++;
			p.waitFor();
			// ------------- REDUCE ---------------------------------
			List <String> arguments_reduce = new ArrayList<String>();
			arguments_reduce.add("ssh");
			arguments_reduce.add("mrodrigues@" + machineToCopy);
			arguments_reduce.add("java");
			arguments_reduce.add("-jar");
			arguments_reduce.add("/tmp/mrodrigues/slave.jar");
			arguments_reduce.add("2");
			arguments_reduce.add(word);
			arguments_reduce.add("/tmp/mrodrigues/maps/SM" + (countKeys - 1) + ".txt");
			arguments_reduce.add("/tmp/mrodrigues/reduces/RM" + (countKeys - 1) + ".txt");
			ProcessBuilder pb_reduce = new ProcessBuilder(arguments_reduce);
			Process p_reduce = pb_reduce.start();
			System.out.println("RM" + (countKeys - 1) + " " +  machineToCopy);
		}
		for (int p = 0; p<runningProcessShufflePrep.size(); p++)
		{
			runningProcessShufflePrep.get(p).waitFor();
		}
	}
	
	// ----------------------------------- FUNCTIONS ------------------------------------------
	
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
	
	// --------------------------------- DISPLAY DICT CONTENT ------------------------------------------------------
	public static void printDict (HashMap dict) {
		for (Object key : dict.keySet()) {
			System.out.println(key + " : " + dict.get(key));
		}
	}
	
	// ------------------------------------ CREATE DIRECTORY ---------------------------------------
	public static void createDir(String fullPathOfDirectoy) throws IOException, InterruptedException {
		ArrayList <Process> runningProcess_dir = new ArrayList <Process>();
		ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", fullPathOfDirectoy);
		Process p = pb.start();
		runningProcess_dir.add(p);
		for (int i=0; i<runningProcess_dir.size(); i++) {
			runningProcess_dir.get(i).waitFor();
		}
	}
	
	// ------------------------------------ CREATE DIRECTORY ---------------------------------------
	public static ArrayList<String> testMachines (String file) throws IOException, InterruptedException
	{
		ArrayList <String>  machinesList = readTxt(file);
		ArrayList <String>  functional = new ArrayList<>(machinesList);
		ArrayList <Process> runningssh = new ArrayList<Process>();
		ArrayList <String>  toRemove = new ArrayList<String>();
		boolean disfunct = true;
		for (int i = 0; i<machinesList.size(); i++)
		{
			ProcessBuilder pb = new ProcessBuilder("ssh", "mrodrigues@" + machinesList.get(i), "hostname");
			Process p = pb.start();
			runningssh.add(p);
		}
		for (int i = 0; i<runningssh.size(); i++)
		{
			boolean b = runningssh.get(i).waitFor(3, TimeUnit.SECONDS);
			if (!b) {
				disfunct = false;
				runningssh.get(i).destroy();
				toRemove.add(machinesList.get(i));
			}
		}
		if (!disfunct) {
			for (int i = (toRemove.size() - 1); i>=0; i--) {
				functional.remove(toRemove.get(i));
			}
		}
		return functional;
	}
	
}
