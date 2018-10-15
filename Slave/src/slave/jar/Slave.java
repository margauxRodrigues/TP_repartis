package slave.jar;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Slave {

	public static void main(String[] args) throws InterruptedException, IOException {
		
		// Create directory
		ArrayList <Process> runningProcess = new ArrayList <Process>();
		ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", "/tmp/mrodrigues/maps");
		Process p = pb.start();
		runningProcess.add(p);
		for (int i=0; i<runningProcess.size(); i++) {
			runningProcess.get(i).waitFor();
		}
		
		//  MAP
		String fileSplit = args[1];
		File newTextFile = new File("/tmp/mrodrigues/maps/UM" + args[1].substring(args[1].length() - 5));
        FileWriter fw = new FileWriter(newTextFile);
		ArrayList<String> splitText = readTxt(fileSplit);
		HashMap<String, Integer> um = new HashMap<String, Integer>();
		for (int i=0; i<splitText.size(); i++) {
			for (String mot : splitText.get(i).split(" ")){
				um.put(mot, 1);
				fw.write(mot + " 1\n");
			}
		}
		fw.close();
		System.out.println("Map done " + args[1]);
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
}
