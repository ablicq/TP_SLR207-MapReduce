package map_reduce;

import java.io.File;
import java.io.IOException;

public class Splitter {

    public static int split(String filePath, String size) {
        ProcessBuilder mkdirPB = new ProcessBuilder("mkdir", "-p", "/tmp/ablicq/splits");
        ProcessBuilder splitPB = new ProcessBuilder("split", "--line-bytes="+size, "--numeric-suffixes", "--additional-suffix=.txt", filePath, "/tmp/ablicq/splits/S");
        try {
            mkdirPB.start().waitFor();
            splitPB.start().waitFor();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
        return new File("/tmp/ablicq/splits").listFiles().length;
    }
}