package map_reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static map_reduce.JobsManager.encode;

public class Reducer {
    private ArrayList<String> hosts;
    private Shuffler shuffler;
    private HashMap<String, ArrayList<Integer>> keySplitMap;
    private HashMap<Integer, String> splitHostMap;

    private HashMap<String, Integer> keyId = new HashMap<>();


    public Reducer(ArrayList<String> hosts, Shuffler shuffler, HashMap<String, ArrayList<Integer>> keySplitMap, HashMap<Integer, String> splitHostMap) {
        this.hosts = hosts;
        this.shuffler = shuffler;
        this.keySplitMap = keySplitMap;
        this.splitHostMap = splitHostMap;
    }


    /**
     * Run the entire reduce phase.
     * <ul>
     *     <li>Assign the maps to the slaves (see map_reduce.Shuffler#shuffle)</li>
     *     <li>Transfer the maps between the slaves</li>
     *     <li>Order the slaves to sort the maps</li>
     *     <li>Order the slaves to reduce the maps</li>
     * </ul>
     */
    public void reduce(){
        System.out.println("Assigning keys to slaves");
        this.shuffler.shuffle();
        this.genKeyIds();
        System.out.println("Transferring maps");
        this.tranferMaps();
        System.out.println("Sorting maps");
        this.runSortMaps();
        System.out.println("Reducing maps");
        this.runReduce();
        System.out.println("Reduce phase finished");
        System.out.println("==================================");
    }


    private String mapNoToLoc(Integer mapNo) {
        String mapsLoc = "/tmp/ablicq/maps";
        return mapsLoc + "/UM" + mapNo + ".txt";
    }

    /**
     * Assign a unique id to each key
     */
    private void genKeyIds() {
        int cpt = 0;
        for(String k :keySplitMap.keySet()){
            keyId.put(k, cpt);
            cpt++;
        }
    }

    /**
     * Transfer all the maps to the right slaves for the reduce phase
     */
    private void tranferMaps(){
        hosts.parallelStream().forEach(host ->
                shuffler.getFilesToTransfer().get(host).parallelStream().forEach(splitNo -> {
                    String src = splitHostMap.get(splitNo) + ":" + mapNoToLoc(splitNo);
                    String dest = host + ":" + mapNoToLoc(splitNo);
                    ProcessBuilder transferPB = new ProcessBuilder("scp", src, dest);
                    try {
                        transferPB.start().waitFor();
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                })
        );
    }

    /**
     * Run the sort map on each slave
     * ie go through all maps to collect the occurrences of the keys
     */
    private void runSortMaps(){
        hosts.parallelStream().forEach(host ->{
            ArrayList<String> sshWrapper = new ArrayList<>(Arrays.asList("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    host));
            shuffler.getMapAssignments().get(host).forEach(key ->{
                ArrayList<String> cmd = new ArrayList<>(sshWrapper);
                cmd.addAll(Arrays.asList("java", "-jar", "/tmp/ablicq/slave.jar", "1", encode(key), keyId.get(key).toString()));
                keySplitMap.get(key).stream().map(Object::toString).forEach(cmd::add);
                ProcessBuilder runMapBuilder = new ProcessBuilder(cmd);
                try{
                    runMapBuilder.start().waitFor();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
    }

    /**
     * Run the reduce on each slave
     * ie count the occurrences of each key
     */
    private void runReduce(){
        hosts.parallelStream().forEach(host ->{
            ArrayList<String> sshWrapper = new ArrayList<>(Arrays.asList("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    host));
            shuffler.getMapAssignments().get(host).forEach(key->{
                ArrayList<String> cmd = new ArrayList<>(sshWrapper);
                cmd.addAll(Arrays.asList("java", "-jar", "/tmp/ablicq/slave.jar", "2", encode(key), keyId.get(key).toString()));
                ProcessBuilder runReduceBuilder = new ProcessBuilder(cmd);
                try{
                    Process runMapProcess=runReduceBuilder.start();
                    runMapProcess.waitFor();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
    }
}
