import java.io.*;
import java.util.Scanner;

public class Mapper {
    public static void map(String inFile, String outFile) throws FileNotFoundException {
        Scanner in = new Scanner(new File(inFile));
        String count = count(in);
        try(PrintWriter out = new PrintWriter(outFile)){
            out.print(count);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String count(Scanner in){
        StringBuilder ret = new StringBuilder();
        while (in.hasNext()){
            String word = in.next();
            ret.append(word).append(" 1\n");
        }
        return ret.toString();
    }
}
