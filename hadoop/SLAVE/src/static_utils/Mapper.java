package static_utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Scanner;

public class Mapper {
    /**
     * Apply the map process to the data in the split of number splitNo, and put the result in the corresponding map file.
     * If N is the splitNo, then the input file is /tmp/ablicq/splits/SN.txt,
     * and the output file is /tmp/ablicq/maps/UMN.txt
     *
     * @param splitNo the number of the split to process
     */
    public static void map(String splitNo) {
        // creates maps folder if it doesn't already exists
        if (Files.notExists(Paths.get("/tmp/ablicq/maps")))
            createMapsFolder();

        // get inFile and outFile, and perform the map
        try (Scanner inFile = splitNoToInFile(splitNo);
             PrintWriter outFile = splitNoToOutFile(splitNo)) {
            count(inFile, outFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Count the words in a document and put the result in another file
     * More precisely, put a line "$word 1" for each $word in the input document
     *
     * @param inFile  where to find the data
     * @param outFile where to put the result
     */
    private static void count(Scanner inFile, PrintWriter outFile) {
        HashSet<String> uniqueKeys = new HashSet<>();
        while (inFile.hasNext()) {
            String word = inFile.next();
            uniqueKeys.add(word);
            outFile.println(word + " 1");
        }
        // send the unique keys to the master through the standard output
        for (String word : uniqueKeys) {
            System.out.println(word);
        }
    }

    /**
     * Create the maps folder in /tmp/maps
     */
    private static void createMapsFolder() {
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", "/tmp/ablicq/maps/");
        try {
            pb.start().waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Scanner splitNoToInFile(String splitNo) throws FileNotFoundException {
        String inPath = String.format("/tmp/ablicq/splits/S%02d.txt", Integer.parseInt(splitNo));
        return new Scanner(new File(inPath));
    }

    private static PrintWriter splitNoToOutFile(String splitNo) throws IOException {
        String outPath = "/tmp/ablicq/maps/UM" + splitNo + ".txt";
        return new PrintWriter(outPath);
    }
}