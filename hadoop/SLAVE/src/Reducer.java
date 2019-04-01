import java.io.*;

public class Reducer {
    public static void reduce(String key, String inNo){
        String inFile = "/tmp/ablicq/maps/SM" + inNo + ".txt";
        String outFile = createOut(inNo);
        int nbKey = 0;
        try(BufferedReader inReader = new BufferedReader(new FileReader(inFile))) {
            nbKey = inReader.lines().mapToInt(l -> Integer.parseInt(l.split("\\s")[1])).sum();
        } catch (IOException e) {e.printStackTrace();}
        try (PrintWriter outPrint = new PrintWriter(new FileWriter(outFile))) {
            outPrint.println(key + " " + nbKey);
        } catch (IOException e) {e.printStackTrace();}
    }

    private static String createOut(String inNo) {
        String outFile = "/tmp/ablicq/reduces/RM"+inNo+".txt";
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", "/tmp/ablicq/reduces/");
        try{
            Process p = pb.start();
            p.waitFor();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return outFile;
    }
}
