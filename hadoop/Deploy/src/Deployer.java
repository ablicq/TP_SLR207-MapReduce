import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class Deployer {
    ArrayList<String> hostsList = new ArrayList<>();

    public Deployer(String configFile) {
        parseHosts(configFile);
    }

    private void parseHosts(String configFile){
        // read the hosts list from the config file
        try (Scanner in = new Scanner(new FileInputStream(configFile))) {
            while (in.hasNextLine()){
                String s = in.nextLine();
                hostsList.add(s.split("\\s")[0]);
            }
        } catch (Exception e) { e.printStackTrace();}
    }


    public void runTest(){
        for(String host : hostsList){
            ProcessBuilder pb = new ProcessBuilder(
                    "ssh",
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

    public void deploy(String jarFile){
        for (String host : hostsList){
            ProcessBuilder pb1 = new ProcessBuilder(
                    "ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host,
                    "mkdir", "-p", "/tmp/ablicq",
                    "&&",
                    "hostname"
            );

            ProcessBuilder pb2 = new ProcessBuilder(
                    "scp", jarFile, "ablicq@"+host+":/tmp/ablicq/slave.jar"
            );

            pb1.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb1.redirectError(ProcessBuilder.Redirect.INHERIT);

            pb2.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb2.redirectError(ProcessBuilder.Redirect.INHERIT);

            try {
                Process p1 = pb1.start();
                p1.waitFor();
                pb2.start();

            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        Deployer deployer = new Deployer("config.txt");
        deployer.runTest();
        deployer.deploy("/tmp/ablicq/SLAVE.jar");
    }
}
