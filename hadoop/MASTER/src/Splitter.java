import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Splitter {
    private ArrayList<String> hosts;
    private HashMap<String, ArrayList<String>> assignments = new HashMap<>();


    /**
     * Generate the splits (TODO), assign slaves to the splits, deploy the splits to the slaves, and order the maps to run
     * @param hosts the hosts of the slave machines
     */
    public Splitter(ArrayList<String> hosts) {
        this.hosts = hosts;
        ArrayList<String> splits = new ArrayList<>(Arrays.asList("/tmp/ablicq/splits/S0.txt", "/tmp/ablicq/splits/S1.txt", "/tmp/ablicq/splits/S2.txt"));
        assignTasks(splits);
    }


    /**
     * Assign each split to a host that will have to process it for the map phase
     * @param splits the paths to the splits to be assigned
     */
    private void assignTasks(ArrayList<String> splits){
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


    /**
     * Deploy the splits ie send each split to the slave that have been assigned to do the map with it
     */
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


    /**
     * Tell each slave to do the map with the split it has been assigned to
     */
    public void runMaps(){
        ArrayList<LinkedBlockingQueue<String>> queues = new ArrayList<>();
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
                    // Run the map
                    Process runMapProcess = runMapBuilder.start();
                    runMapProcess.waitFor();

                    // prepare the output reading
                    BufferedInputStream pStd = new BufferedInputStream(runMapProcess.getInputStream());
                    LinkedBlockingQueue<String> stdTimeOutQueue = new LinkedBlockingQueue<>();
                    ReadThread readStd = new ReadThread(pStd, stdTimeOutQueue);
                    readStd.start();

                    // read the output
                    String val = "";
                    while(val != null)
                    {
                        System.out.print(val);
                        val = stdTimeOutQueue.poll(10000, TimeUnit.MILLISECONDS);
                    }
                    readStd.stopRun();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
    }


    public static void main(String[] args) {
        Splitter splitter = new Splitter(new ArrayList<>(Arrays.asList(args)));
        splitter.deploy();
        splitter.runMaps();
    }
}
