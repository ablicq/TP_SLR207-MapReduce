import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class Splitter {
    private ArrayList<String> hosts;
    private HashMap<String, ArrayList<String>> assignments = new HashMap<>();

    public Splitter(ArrayList<String> hosts) {
        this.hosts = hosts;
        ArrayList<String> splits = new ArrayList<>(Arrays.asList("/tmp/ablicq/splits/S0.txt", "/tmp/ablicq/splits/S1.txt", "/tmp/ablicq/splits/S2.txt"));
        assignTasks(splits);
    }


    public void assignTasks(ArrayList<String> splits){
        int nbHosts = hosts.size();
        int nbSplits = splits.size();
        for(int i = 0; i < nbSplits; ++i){
            String h = hosts.get(i % nbHosts);
            if(assignments.containsKey(h)) {
                assignments.get(h).add(splits.get(i));
            } else {
                assignments.put(h, new ArrayList<String>(Collections.singleton(splits.get(i))));
            }
        }
    }


    public void deploy(){
        hosts.parallelStream().forEach((host) -> {

            ProcessBuilder mkdirPB = new ProcessBuilder(
                    "ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host,
                    "mkdir", "-p", "/tmp/ablicq/splits"
            );


            ArrayList<String> copyCmd = new ArrayList<>();
            copyCmd.add("scp");
            copyCmd.addAll(assignments.get(host));
            copyCmd.add("ablicq@"+host+":/tmp/ablicq/splits/");

            ProcessBuilder scpPB = new ProcessBuilder(
                    copyCmd
            );

            mkdirPB.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            mkdirPB.redirectError(ProcessBuilder.Redirect.INHERIT);

            scpPB.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            scpPB.redirectError(ProcessBuilder.Redirect.INHERIT);

            try {
                Process mkdir = mkdirPB.start();
                mkdir.waitFor();
                scpPB.start();
            } catch (IOException | InterruptedException e){
                e.printStackTrace();
            }
        });
    }


    public void runMaps(){
        hosts.parallelStream().forEach(host ->{
            ArrayList<String> sshWrapper = new ArrayList<>(Arrays.asList("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host));
            assignments.get(host).forEach(split ->{
                ArrayList<String> cmd = new ArrayList<>(sshWrapper);
                cmd.addAll(Arrays.asList("java", "-jar", "/tmp/ablicq/slave.jar", "0", split));
                ProcessBuilder runMapBuilder = new ProcessBuilder(cmd);
                try{
                    Process runMapProcess = runMapBuilder.start();
                    runMapProcess.waitFor();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
                // TODO: use ReadThread to get output
            });
        });
    }


    public static void main(String[] args) {
        Splitter splitter = new Splitter(new ArrayList<>(Arrays.asList(args)));
        splitter.deploy();
        splitter.runMaps();
    }
}
