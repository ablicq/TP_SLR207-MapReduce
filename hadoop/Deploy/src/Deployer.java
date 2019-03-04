import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;


public class Deployer {
    /**
     * the list of hosts indicated in the configuration file
     */
    ArrayList<String> hostsList = new ArrayList<>();

    /**
     * Constructor of the Deployer.
     * Parse the given config file to the hostsList
     * @param configFile the config file containing the hosts to which we wish to deploy
     */
    public Deployer(String configFile) {
        parseHosts(configFile);
    }

    /**
     * Parse the given config file to the hostsList
     * @param configFile the config file containing the hosts to which we wish to deploy
     */
    private void parseHosts(String configFile){
        // read the hosts list from the config file
        try (Scanner in = new Scanner(new FileInputStream(configFile))) {
            while (in.hasNextLine()){
                String s = in.nextLine();
                hostsList.add(s.split("\\s")[0]);
            }
        } catch (Exception e) { e.printStackTrace();}
    }


    /**
     * Send the command 'hostname' to every host to test the connection
     */
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

    /**
     * copy the given jarFile to every host at /tmp/ablicq/slave.jar
     * create the directory if needed
     * @param jarFile the jar file to deploy
     */
    public void deploy(String jarFile){
        for (String host : hostsList){
            ProcessBuilder pb1 = new ProcessBuilder(
                    "ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host,
                    "mkdir", "-p", "/tmp/ablicq"
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
        deployer.deploy("/tmp/ablicq/SLAVE.jar");
    }
}
