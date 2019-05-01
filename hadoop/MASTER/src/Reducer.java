import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class Reducer {
    private ArrayList<String> hosts;
    private Shuffler shuffler;

    private HashMap<String, Integer> keyId = new HashMap<>();

    private HashMap<String, ArrayList<Integer>> keySplitMap;
    private HashMap<Integer, String> splitHostMap;

    public Reducer(ArrayList<String> hosts, Shuffler shuffler, HashMap<String, ArrayList<Integer>> keySplitMap, HashMap<Integer, String> splitHostMap) {
        this.hosts = hosts;
        this.shuffler = shuffler;
        this.keySplitMap = keySplitMap;
        this.splitHostMap = splitHostMap;
    }

    public void reduce(){
        this.shuffler.shuffle();
        this.genKeyIds();
        this.tranferMaps();
        this.runSortMaps();
        this.runReduce();
    }

    /**
     * Assign a unique id to each key
     */
    private String mapNoToLoc(Integer mapNo) {
        String mapsLoc = "/tmp/ablicq/maps";
        return mapsLoc + "/UM" + mapNo + ".txt";
    }

    private void genKeyIds() {
        int cpt = 0;
        for(String k :keySplitMap.keySet()){
            keyId.put(k, cpt);
            cpt++;
        }
    }

    private void tranferMaps(){
        hosts.parallelStream().forEach(host ->
                shuffler.getFilesToTransfer().get(host).parallelStream().forEach(splitNo -> {
                    String src = splitHostMap.get(splitNo) + ":" + mapNoToLoc(splitNo);
                    String dest = host + ":" + mapNoToLoc(splitNo);
                    ProcessBuilder transferPB = new ProcessBuilder("scp", "ablicq@"+src, "ablicq@"+dest);
                    try {
                        transferPB.start().waitFor();
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                })
        );
    }

    private void runSortMaps(){
        hosts.parallelStream().forEach(host ->{
            ArrayList<String> sshWrapper = new ArrayList<>(Arrays.asList("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host));
            shuffler.getMapAssignments().get(host).forEach(key ->{
                ArrayList<String> cmd = new ArrayList<>(sshWrapper);
                cmd.addAll(Arrays.asList("java", "-jar", "/tmp/ablicq/slave.jar", "1", key, keyId.get(key).toString()));
                keySplitMap.get(key).stream().map(Object::toString).forEach(cmd::add);
                //cmd.forEach(System.out::println);
                ProcessBuilder runMapBuilder = new ProcessBuilder(cmd);
                try{
                    // Run the map
                    Process runMapProcess = runMapBuilder.start();
                    runMapProcess.waitFor();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });
        });
    }

    private void runReduce(){
        hosts.parallelStream().forEach(host ->{
            ArrayList<String> sshWrapper = new ArrayList<>(Arrays.asList("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host));
            shuffler.getMapAssignments().get(host).forEach(key->{
                ArrayList<String> cmd = new ArrayList<>(sshWrapper);
                cmd.addAll(Arrays.asList("java", "-jar", "/tmp/ablicq/slave.jar", "2", key, keyId.get(key).toString()));
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
