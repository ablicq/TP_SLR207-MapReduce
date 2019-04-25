import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class JobsManager {
    private ArrayList<String> hosts = new ArrayList<>();
    private HashMap<String, ArrayList<Integer>> splitAssignments = new HashMap<>();
    private String splitsLoc = "/tmp/ablicq/splits";
    private String mapsLoc = "/tmp/ablicq/maps";
    private String reducesLoc = "/tmp/ablicq/reduces";
    private Integer nbSplits = 3;

    private HashMap<String, ArrayList<Integer>> keySplitMap = new HashMap<>();
    private HashMap<Integer, String> splitHostMap = new HashMap<>();
    private HashMap<String, ArrayList<String>> mapAssignments = new HashMap<>();
    private HashMap<String, HashSet<Integer>> filesToTransfer = new HashMap<>();

    private HashMap<String, Integer> keyId = new HashMap<>();

    /**
     * Assign slaves to the splits, deploySplits the splits to the slaves, and order the maps to run
     * @param hosts the hosts of the slave machines
     */
    public JobsManager(ArrayList<String> hosts) {
        this.hosts = hosts;
        assignSplits();
    }


    /**
     * Assign slaves to the splits, deploySpldeployits the splits to the slaves, and order the maps to run
     * @param configFile a path to a file containing the hostnames of the slaves
     */
    public JobsManager(String configFile){
        parseHosts(configFile);
        assignSplits();
    }


    private String splitNoToLoc(Integer splitNo) {
        return splitsLoc + "/S" + splitNo + ".txt";
    }

    private String mapNoToLoc(Integer mapNo) {
        return mapsLoc + "/UM" + mapNo + ".txt";
    }

    private String smNoToLoc(Integer smNo) {
        return mapsLoc + "/SM" + smNo + ".txt";
    }

    private String reduceNoToLoc(Integer reduceNo){
        return reducesLoc + "/R" + reduceNo + ".txt";
    }


    /**
     * Parse the given config file to the hosts list
     * @param configFile the config file containing the hosts to which we wish to deploySplits
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
    private void assignSplits(){
        int nbHosts = hosts.size();
        for(int i = 0; i < nbSplits; ++i){
            String h = hosts.get(i % nbHosts);
            if(splitAssignments.containsKey(h)) {
                splitAssignments.get(h).add(i);
            } else {
                splitAssignments.put(h, new ArrayList<>(Collections.singleton(i)));
            }
            splitHostMap.put(i, h);
        }
    }


    /**
     * Deploy the splits ie send each split to the slave that have been assigned to do the map with it
     */
    public void deploySplits(){
        hosts.parallelStream().forEach((host) -> {

            ProcessBuilder mkdirPB = new ProcessBuilder(
                    "ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host,
                    "mkdir", "-p", "/tmp/ablicq/splits"
            );


            var splitLocs = new ArrayList<String>();
            splitAssignments.get(host).forEach(i -> splitLocs.add(splitNoToLoc(i)));
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
            splitAssignments.get(host).forEach(split ->{
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
                            if(keySplitMap.containsKey(key)) {
                                keySplitMap.get(key).add(split);
                            } else {
                                keySplitMap.put(key, new ArrayList<>(Collections.singleton(split)));
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
        System.out.println("=====================================");
        System.out.println("Map phase finished");
        System.out.println("key -> splits:");
        keySplitMap.forEach((key, value) -> {System.out.print(key + " -> "); value.forEach(i -> System.out.print(i + " ")); System.out.print("\n");});
        System.out.println("split -> host:");
        splitHostMap.forEach((key, value) -> System.out.println(key + " -> " + value));
        System.out.println("=====================================");
    }

    //******************************************************************************************************************
    //******************************************************************************************************************
    //                                        methods for the SHUFFLE phase
    //******************************************************************************************************************
    //******************************************************************************************************************

    /**
     * Assign the reduce tasks to the hosts for the reduce phase in a way that minimizes the assignment complexity
     */
    public void shuffle(){
        // for each key, look for the host minimizing the assignment complexity
        // and assign the key to this host
        keySplitMap.keySet().forEach(key->{
            String host = hosts.get(0);
            int minVal = assignmentComplexity(key, host);
            for(String h : hosts){
                int otherVal = assignmentComplexity(key, h);
                if(minVal > otherVal){
                    minVal = otherVal;
                    host = h;
                }
            }
            assignMap(key, host);
        });

        // print for debug
        System.out.println("Shuffle phase finished");
        System.out.println("Assignments:");
        mapAssignments.forEach((h, k) -> System.out.println(h + " -> " + k));
        System.out.println("File transfers");
        filesToTransfer.forEach((key, value) -> {System.out.print(key + " -> "); value.forEach(i -> System.out.print(i + " ")); System.out.print("\n");});
    }

    /**
     * Assign a key to a host for the reduce phase.
     * Add the key to the list of assignments to the host and
     * update the set of files to send to the host
     * @param key the key to assign
     * @param host the host to which the key is assigned
     */
    private void assignMap(String key, String host){
        // add the key to the assignments of the host
        if(!mapAssignments.containsKey(host)){
            mapAssignments.put(host, new ArrayList<>(Collections.singleton(key)));
        } else {
            mapAssignments.get(host).add(key);
        }
        // add the files to transfer to the host in the set
        for(int mapNo : keySplitMap.get(key)){
            if(!splitAssignments.get(host).contains(mapNo)){
                // add the file to the set of files to transfer
                if(!filesToTransfer.containsKey(host)){
                    filesToTransfer.put(host, new HashSet<>(Collections.singleton(mapNo)));
                } else {
                    filesToTransfer.get(host).add(mapNo);
                }
            }
        }
    }

    /**
     * Compute the complexity to assign the reducing of some key to some host.
     * it is computed as the number of tasks already assigned to the host plus
     * The number of files to transfer to the host to be able to reduce the key.
     * @param key the key to be assigned
     * @param host the host to which assign the key
     * @return the total complexity
     */
    private int assignmentComplexity(String key, String host) {
        int hostBusiness = mapAssignments.containsKey(host) ? mapAssignments.get(host).size() : 0;
        int nbFilesToTransfer = 0;
        for (Integer mapNo : keySplitMap.get(key)) {
            if(!splitAssignments.get(host).contains(mapNo) &&
                    !filesToTransfer.getOrDefault(host, new HashSet<>()).contains(mapNo)){
                nbFilesToTransfer++;
            }
        }
        return hostBusiness + nbFilesToTransfer;
    }


    //******************************************************************************************************************
    //******************************************************************************************************************
    //                                        methods for the REDUCE phase
    //******************************************************************************************************************
    //******************************************************************************************************************


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

    public void tranferMaps(){
        genKeyIds();
        hosts.parallelStream().forEach(host ->
                filesToTransfer.get(host).parallelStream().forEach(splitNo -> {
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

    public void runSortMaps(){
        hosts.parallelStream().forEach(host ->{
            ArrayList<String> sshWrapper = new ArrayList<>(Arrays.asList("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host));
            mapAssignments.get(host).forEach(key ->{
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

    public void runReduce(){
        hosts.parallelStream().forEach(host ->{
            ArrayList<String> sshWrapper = new ArrayList<>(Arrays.asList("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host));
            mapAssignments.get(host).forEach(key->{
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

    public static void main(String[] args) {
        JobsManager jobsManager = new JobsManager(args[0]);
        jobsManager.deploySplits();
        jobsManager.runMaps();
        jobsManager.shuffle();
        jobsManager.tranferMaps();
        jobsManager.runSortMaps();
        jobsManager.runReduce();
    }
}
