import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SplitsManager {
    private ArrayList<String> hosts = new ArrayList<>();
    private HashMap<String, ArrayList<Integer>> assignments = new HashMap<>();
    private String splitsLoc = "/tmp/ablicq/splits";
    private String mapsLoc = "/tmp/ablicq/maps";
    private Integer nbSplits = 3;
    private HashMap<String, ArrayList<Integer>> keyHostMap = new HashMap<>();


    /**
     * Generate the splits (TODO), assign slaves to the splits, deploy the splits to the slaves, and order the maps to run
     * @param hosts the hosts of the slave machines
     */
    public SplitsManager(ArrayList<String> hosts) {
        this.hosts = hosts;
        ArrayList<String> splits = new ArrayList<>(Arrays.asList("/tmp/ablicq/splits/S0.txt", "/tmp/ablicq/splits/S1.txt", "/tmp/ablicq/splits/S2.txt"));
        assignTasks();
    }


    /**
     * Generate the splits (TODO), assign slaves to the splits, deploy the splits to the slaves, and order the maps to run
     * @param configFile a path to a file containing the hostnames of the slaves
     */
    public SplitsManager(String configFile){
        parseHosts(configFile);
        ArrayList<String> splits = new ArrayList<>(Arrays.asList("/tmp/ablicq/splits/S0.txt", "/tmp/ablicq/splits/S1.txt", "/tmp/ablicq/splits/S2.txt"));
        assignTasks();
    }


    private String splitNoToLoc(Integer splitNo) {
        return splitsLoc + "/S" + splitNo + ".txt";
    }

    private String mapNoToLoc(Integer mapNo) {
        return mapsLoc + "/UM" + mapNo + ".txt";
    }

    /**
     * Parse the given config file to the hosts list
     * @param configFile the config file containing the hosts to which we wish to deploy
     */
    private void parseHosts(String configFile) {
        // read the hosts list from the config file
        try (Scanner in = new Scanner(new FileInputStream(configFile))) {
            while (in.hasNextLine()){
                String s = in.nextLine();
                hosts.add(s.split("\\s")[0]);
            }
        } catch (Exception e) { e.printStackTrace();}
    }


    /**
     * Assign each split to a host that will have to process it for the map phase
     */
    private void assignTasks(){
        int nbHosts = hosts.size();
        for(int i = 0; i < nbSplits; ++i){
            String h = hosts.get(i % nbHosts);
            if(assignments.containsKey(h)) {
                assignments.get(h).add(i);
            } else {
                assignments.put(h, new ArrayList<>(Collections.singleton(i)));
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


            var splitLocs = new ArrayList<String>();
            assignments.get(host).forEach(i -> splitLocs.add(splitNoToLoc(i)));
            ArrayList<String> copyCmd = new ArrayList<>();
            copyCmd.add("scp");
            copyCmd.addAll(splitLocs);
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
                cmd.addAll(Arrays.asList("java", "-jar", "/tmp/ablicq/slave.jar", "0", splitNoToLoc(split)));
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
                    String key = "";
                    while(val != null)
                    {
                        if (val.equals("\n")) {
                            if(keyHostMap.containsKey(key)) {
                                keyHostMap.get(key).add(split);
                            } else {
                                keyHostMap.put(key, new ArrayList<>(Collections.singleton(split)));
                            }
                            key = "";
                        } else {
                            key += val;
                        }
                        //System.out.print(val);
                        val = stdTimeOutQueue.poll(1000, TimeUnit.MILLISECONDS);
                    }
                    readStd.stopRun();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
        keyHostMap.forEach((key, value) -> {System.out.print(key + ": "); value.forEach(i -> System.out.print(i + " ")); System.out.print("\n");});
    }


    public static void main(String[] args) {
        SplitsManager splitsManager = new SplitsManager(args[0]);
        splitsManager.deploy();
        splitsManager.runMaps();
    }
}
