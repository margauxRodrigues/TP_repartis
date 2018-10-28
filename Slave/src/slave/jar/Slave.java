package slave.jar;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class Slave {

	public static void main(String[] args) throws InterruptedException, IOException {
		
		if (args[0].equals("0")) // Map
		{
			createDir("/tmp/mrodrigues/maps");
			HashMap<String, Integer> um = map(args[1]);
		
			for (Object key : um.keySet()) {
				System.out.println(key);
			}
		}
		else if (args[0].equals("1")) // Shuffle
		{
			createDir("/tmp/mrodrigues/maps");
			HashMap< String, Integer > sortedMaps = shuffle(args[1], args[2], Arrays.copyOfRange(args, 3, args.length));
			System.out.println("Succes for shuffle phase");
		}
		else if (args[0].equals("2")) // Reduce
		{
			createDir("/tmp/mrodrigues/reduces");
			reduce(args[1], args[2], args[3]);
		}
		else {System.out.println("Echec,fonction non reconnue");}
	}
	
	// ----------------------------- MAP FUNCTION ----------------------------------
	public static HashMap<String, Integer> map(String InputSplitFile) throws IOException {
		String fileSplit = InputSplitFile;
		File newTextFile = new File("/tmp/mrodrigues/maps/UM" + fileSplit.substring(fileSplit.length() - 5));
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
		return um;
	}
	
	// ---------------------------------- SHUFFLE ----------------------------------------------
	public static HashMap< String, Integer > shuffle(String keyWord, String outputFile, String[] UMlist) throws IOException{
		HashMap<String, Integer> sortedMap = new HashMap< String, Integer >();
		File newTextFile = new File(outputFile);
        FileWriter fw = new FileWriter(newTextFile);
		for (int i=0; i<UMlist.length; i++) {
			ArrayList< String > fileContent = readTxt(UMlist[i]);
			for (int j=0; j<fileContent.size(); j++) {
				if (fileContent.get(j).split(" ")[0].equals(keyWord)){
					sortedMap.put(keyWord, Integer.parseInt(fileContent.get(j).split(" ")[1]));
					fw.write(keyWord + " " + sortedMap.get(keyWord).toString() + "\n");
				}
			}
		}
		fw.close();
		return sortedMap;
	}
	
	// ---------------------------------- REDUCE ----------------------------------------------
	public static void reduce(String keyword, String inputSortedMap, String output) throws IOException{
		ArrayList<String> text = readTxt(inputSortedMap);
		int count = 0;
		for (int i = 0; i<text.size(); i++)
		{
			count += Integer.parseInt(text.get(i).split(" ")[1]);
		}
		File newTextFile = new File(output);
        FileWriter fw = new FileWriter(newTextFile);
        fw.write(keyword + " " + count);
        fw.close();
	}
	
	// ----------------------------------- FONCTION READ TXT -------------------------------------
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
	
	//------------------------------------- TXT FILE TO HASHMAP ------------------------------------
	public static HashMap<String, Integer> convertTxtToHashMap(String filename) throws IOException {
		HashMap< String, Integer > result = new HashMap< String, Integer >();
		ArrayList< String > readFile = readTxt(filename);
		for (int i =0; i<readFile.size(); i++) {
			String[] split = readFile.get(i).split(" ");
			result.put(split[0], Integer.parseInt((split[1])));
		}
		return result;
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
	
}
