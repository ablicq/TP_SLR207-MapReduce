package static_utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Scanner;

import static static_utils.Mapper.decode;

public class MapSorter {
    /**
     * Collect the occurrences of a key in the maps, and put these on a file.
     * @param key the key to look for
     * @param outNo the id of the key for the master (determines the output file)
     * @param maps the maps in which to look for the key
     */
    public static void sortMaps(String key, String outNo, ArrayList<String> maps) {
        String decodedKey = decode(key);
        try(PrintWriter outPrinter = outNoToFile(outNo)){
            for(String m : maps) {
                try(Scanner mapReader = mapNoToFile(m)){
                    while(mapReader.hasNextLine()){
                        String l = mapReader.nextLine();
                        if(l.contains(decodedKey))
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
}

