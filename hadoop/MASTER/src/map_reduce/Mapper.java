package map_reduce;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static map_reduce.JobsManager.decode;

public class Mapper {
    private ArrayList<String> hosts;
    private int nbSplits;

    private HashMap<String, ArrayList<Integer>> splitAssignments = new HashMap<>();
    private HashMap<Integer, String> splitHostMap = new HashMap<>();
    private HashMap<String, ArrayList<Integer>> keySplitMap = new HashMap<>();


    public Mapper(ArrayList<String> hosts, int nbSplits) {
        this.hosts = hosts;
        this.nbSplits = nbSplits;
    }


    public HashMap<Integer, String> getSplitHostMap() {
        return splitHostMap;
    }

    public HashMap<String, ArrayList<Integer>> getKeySplitMap() {
        return keySplitMap;
    }

    public HashMap<String, ArrayList<Integer>> getSplitAssignments() {
        return splitAssignments;
    }


    /**
     * Run the entire map process on all the machines.
     * <ul>
     *     <li>Assign the splits to the hosts</li>
     *     <li>Deploy the splits to the assigned hosts</li>
     *     <li>Order each slave to run the map process</li>
     * </ul>
     */
    public void map(){
        System.out.println("Assigning splits");
        this.assignSplits();
        System.out.println("Deploying splits");
        this.deploySplits();
        System.out.println("Running maps");
        this.runMaps();
        System.out.println("Map phase finished");
        System.out.println("==================================");
    }


    /**
     * Assign each split to a host that will have to process it for the map phase
     */
    private void assignSplits() {
        int nbHosts = hosts.size();
        for (int i = 0; i < nbSplits; ++i) {
            String h = hosts.get(i % nbHosts);
            if (splitAssignments.containsKey(h)) {
                splitAssignments.get(h).add(i);
            } else {
                splitAssignments.put(h, new ArrayList<>(Collections.singleton(i)));
            }
            splitHostMap.put(i, h);
        }

        for(String h : hosts){
            if(!splitAssignments.containsKey(h)){
                splitAssignments.put(h, new ArrayList<>());
            }
        }
    }

    /**
     * Deploy the splits ie send each split to the slave that have been assigned to do the map with it
     */
    private void deploySplits() {
        hosts.parallelStream().forEach((host) -> {

            // if no map job assigned to host, return
            if(splitAssignments.get(host).isEmpty()) return;

            ProcessBuilder mkdirPB = new ProcessBuilder(
                    "ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    host,
                    "mkdir", "-p", "/tmp/ablicq/splits"
            );


            var splitLocs = new ArrayList<String>();
            splitAssignments.get(host).forEach(i -> splitLocs.add(splitNoToLoc(i)));
            ArrayList<String> copyCmd = new ArrayList<>();
            copyCmd.add("scp");
            copyCmd.addAll(splitLocs);
            copyCmd.add(host + ":/tmp/ablicq/splits/");

            ProcessBuilder scpPB = new ProcessBuilder(
                    copyCmd
            );

            try {
                mkdirPB.start().waitFor();
                scpPB.start().waitFor();
            } catch (IOException | InterruptedException e) { e.printStackTrace(); }
        });
    }

    /**
     * Tell each slave to do the map with the split it has been assigned to
     */
    private void runMaps(){
        hosts.parallelStream().forEach(host ->{
            ArrayList<String> sshWrapper = new ArrayList<>(Arrays.asList("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    host));
            splitAssignments.get(host).forEach(split ->{
                ArrayList<String> cmd = new ArrayList<>(sshWrapper);
                cmd.addAll(Arrays.asList("java", "-jar", "/tmp/ablicq/slave.jar", "0", split.toString()));
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
                            String unescaped_key = decode(key);
                            if(keySplitMap.containsKey(unescaped_key)) {
                                keySplitMap.get(unescaped_key).add(split);
                            } else {
                                keySplitMap.put(unescaped_key, new ArrayList<>(Collections.singleton(split)));
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
    }

    private String splitNoToLoc(Integer splitNo) {
        String splitsLoc = "/tmp/ablicq/splits";
        return splitsLoc + String.format("/S%02d.txt", splitNo);
    }
}
