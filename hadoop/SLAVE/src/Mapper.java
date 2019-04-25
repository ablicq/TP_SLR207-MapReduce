import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;

public class Mapper {
    /**
     * Count the words in a document and put the result in another file
     * More precisely, put a line "$word 1" for each $word in the input document
     * @param inFile where to fin the data
     * @param outFile where to put the result
     * @throws FileNotFoundException if either the input or output files are not found
     */
    private static void count(String inFile, String outFile) throws FileNotFoundException {
        Scanner in = new Scanner(new File(inFile));
        PrintWriter out = new PrintWriter(outFile);
        HashSet<String> uniqueKeys = new HashSet<>();
        while (in.hasNext()){
            String word = in.next();
            uniqueKeys.add(word);
            out.println(word + " 1");
        }
        // send the unique keys to the master through the standard output
        for(String word : uniqueKeys){
            System.out.println(word);
        }
        in.close();
        out.close();
    }

    /**
     * From the name of the input file, infer the name of the output file,
     * and create the directory to store the map files
     * @param inFile the name of the input file
     * @return the name of the output file
     */
    private static String createOut(String inFile){
        int l = inFile.length();
        String splitNo = inFile.substring(l-5, l-4);
        String outFile = "/tmp/ablicq/maps/UM"+splitNo+".txt";
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", "/tmp/ablicq/maps/");
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        try{
            Process p = pb.start();
            p.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return outFile;
    }

    /**
     * Apply the map process to the data in the input file and put the result in a file inferred from the input file
     * The input file has to be of the form "/tmp/ablicq/splits/SN.txt" where N is the id of the processed split
     * The output is then "/tmp/ablicq/maps/UMN.txt"
     * @param inFile the path to the input file
     */
    public static void map(String inFile){
        // get the name of the outFile according to the name of the inFile
        String outFile = createOut(inFile);
        try {
            count(inFile, outFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void sortMaps(String key, String outNo, ArrayList<String> maps) {
        try(PrintWriter outPrinter = outNoToFile(outNo)){
            for(String m : maps) {
                System.out.println(m);
                System.out.println(mapNoToFile(m));
                try(Scanner mapReader = mapNoToFile(m)){
                    while(mapReader.hasNextLine()){
                        String l = mapReader.nextLine();
                        if(l.contains(key))
                            outPrinter.println(l);
                    }
                } catch (FileNotFoundException e) { e.printStackTrace(); }
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    private static Scanner mapNoToFile(String mapNo) throws FileNotFoundException {
        String mapFile = "/tmp/ablicq/maps/UM" + mapNo + ".txt";
        return new Scanner(new File(mapFile));
    }

    private static PrintWriter outNoToFile(String outNo) throws IOException {
        String outFile = "/tmp/ablicq/maps/SM" + outNo + ".txt";
        return new PrintWriter(outFile);
    }


    public static void main(String[] args) {
        map("/tmp/ablicq/splits/S2.txt");
    }
}
