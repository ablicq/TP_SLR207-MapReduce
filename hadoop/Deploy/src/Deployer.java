import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class Deployer {
    private String configFile;

    public Deployer(String configFile) {
        this.configFile = configFile;
    }

    public void runTest(){
        // read the hosts list from the config file
        ArrayList<String> hostsList = new ArrayList<>();
        try (Scanner in = new Scanner(new FileInputStream(configFile))) {
            while (in.hasNextLine()){
                String s = in.nextLine();
                hostsList.add(s.split("\\s")[0]);
            }
        } catch (Exception e) { e.printStackTrace();}

        for(String host : hostsList){
            ProcessBuilder pb = new ProcessBuilder("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host,
                    "hostname");
            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);

            try {
                Process p = pb.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        Deployer test = new Deployer("config.txt");
        test.runTest();
    }
}
