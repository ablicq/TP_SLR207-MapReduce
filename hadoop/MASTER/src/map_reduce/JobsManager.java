package map_reduce;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class JobsManager {
    private ArrayList<String> hosts = new ArrayList<>();

    private Mapper mapper;
    private Reducer reducer;

    /**
     * Create a new map_reduce.JobsManager, managing the jobs of the given hosts, and the given splits
     * @param hosts the hosts of the slave machines
     * @param nbSplits the number of splits to be processed
     */
    public JobsManager(ArrayList<String> hosts, int nbSplits) {
        this.hosts = hosts;
        this.mapper = new Mapper(hosts, nbSplits);
    }


    /**
     * Create a new map_reduce.JobsManager, managing the jobs of the given hosts, and the given splits
     * @param configFile a path to a file containing the hostnames of the slaves
     * @param nbSplits the number of splits to be processed
     */
    public JobsManager(String configFile, int nbSplits){
        parseHosts(configFile);
        this.mapper = new Mapper(hosts, nbSplits);
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
                // ignore blank lines and comments
                if(!s.matches("\\s*|#.*"))
                    hosts.add(s.split("\\s")[0]);
            }
        } catch (Exception e) { e.printStackTrace();}
    }

    /**
     * Run the entire Map-Reduce algorithm
     */
    public void runAll(){
        mapper.map();
        Shuffler shuffler = new Shuffler(hosts, mapper.getKeySplitMap(), mapper.getSplitAssignments());
        reducer = new Reducer(hosts, shuffler, mapper.getKeySplitMap(), mapper.getSplitHostMap());
        reducer.reduce();
    }


    /**
     * Fetch the results from the slaves
     * @return A HashMap containing the word count
     */
    public HashMap<String, Integer> fetchResults(){
        HashMap<String, Integer> ret = new HashMap<>();
        hosts.parallelStream().map(host -> new ProcessBuilder("ssh",
                "-o", "UserKnownHostsFile=/dev/null",
                "-o", "StrictHostKeyChecking=no",
                host,
                "cat", "/tmp/ablicq/reduces/*")).forEach(catPB -> {
            try {
                Process catP = catPB.start();

                BufferedInputStream pStd = new BufferedInputStream(catP.getInputStream());
                LinkedBlockingQueue<String> stdTimeOutQueue = new LinkedBlockingQueue<>();
                ReadThread readStd = new ReadThread(pStd, stdTimeOutQueue);
                readStd.start();

                String val = "";
                String l = "";
                while (val != null) {
                    if (val.equals("\n")) {
                        String[] a = l.split("\\s");
                        ret.put(a[0], Integer.parseInt(a[1]));
                        l = "";
                    } else {
                        l += val;
                    }
                    val = stdTimeOutQueue.poll(1000, TimeUnit.MILLISECONDS);
                }
                readStd.stopRun();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        return ret;
    }

    /**
     * Encode a UTF8 string using BASE64 to avoid transmitting special characters
     * @param str the string to encode
     * @return the encoded string
     */
    static String encode(String str) {
        byte[] strBytes = str.getBytes();
        byte[] encodedBytes = Base64.getEncoder().encode(strBytes);
        return new String(encodedBytes);
    }

    /**
     * Decode a BASE64 encoded string to a UTF8 string
     * @param str the encoded string
     * @return the decoded string
     */
    static String decode(String str) {
        byte[] strBytes = str.getBytes();
        byte[] decodedBytes = Base64.getDecoder().decode(strBytes);
        return new String(decodedBytes, Charset.forName("UTF8"));
    }
}
