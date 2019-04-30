import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Reducer {
    /**
     * Reduce the sorted maps by counting the occurrences of the key in the inFile and put it in the outFile
     * @param key the key to reduce
     * @param inNo the index of the key to reduce (to get the inFile and outFile paths)
     */
    public static void reduce(String key, String inNo){
        // create reduces folder if it doesn't exist
        if (Files.notExists(Paths.get("/tmp/ablicq/reduces")))
            createReducesFolder();

        // get inFile, outFile, and perform the map
        try(BufferedReader inReader = createIn(inNo);
            PrintWriter outPrint = createOut(inNo))
        {
            // compute the sum of the second characters of the lines of the input file
            int nbKey = inReader.lines()
                    .mapToInt(l -> Integer.parseInt(l.split("\\s")[1]))
                    .sum();
            // format the result and put it in the output file
            outPrint.println(key + " " + nbKey);
        }
        catch (IOException e) {e.printStackTrace();}
    }


    /**
     * create the reduce folder in the directory /tmp/ablicq
     */
    private static void createReducesFolder() {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", "/tmp/ablicq/reduces/");
        try{ pb.start().waitFor(); }
        catch (IOException | InterruptedException e) { e.printStackTrace(); }
    }


    private static BufferedReader createIn(String inNo) throws FileNotFoundException {
        String inFile = "/tmp/ablicq/maps/SM" + inNo + ".txt";
        return new BufferedReader(new FileReader(inFile));
    }

    private static PrintWriter createOut(String inNo) throws IOException {
        String outFile = "/tmp/ablicq/reduces/RM"+inNo+".txt";
        return new PrintWriter(new FileWriter(outFile));
    }
}
