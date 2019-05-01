import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class JobsManager {
    private ArrayList<String> hosts = new ArrayList<>();

    private Mapper mapper;
    private Reducer reducer;

    /**
     * Assign slaves to the splits, deploySplits the splits to the slaves, and order the maps to run
     * @param hosts the hosts of the slave machines
     */
    public JobsManager(ArrayList<String> hosts, int nbSplits) {
        this.hosts = hosts;
        this.mapper = new Mapper(hosts, nbSplits);
    }


    /**
     * Assign slaves to the splits, deploy the splits to the slaves, and order the maps to run
     * @param configFile a path to a file containing the hostnames of the slaves
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
                hosts.add(s.split("\\s")[0]);
            }
        } catch (Exception e) { e.printStackTrace();}
    }

    public void runAll(){
        mapper.map();
        Shuffler shuffler = new Shuffler(hosts, mapper.getKeySplitMap(), mapper.getSplitAssignments());
        reducer = new Reducer(hosts, shuffler, mapper.getKeySplitMap(), mapper.getSplitHostMap());
        reducer.reduce();
    }


    public void fetchResults(){
        System.out.println("=====================================");
        System.out.println("Final Results:");
        hosts.parallelStream().forEach(host ->{
            ProcessBuilder catPB = new ProcessBuilder("ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host,
                    "cat", "/tmp/ablicq/reduces/*");
            try {
                Process catP = catPB.start();

                BufferedInputStream pStd = new BufferedInputStream(catP.getInputStream());
                LinkedBlockingQueue<String> stdTimeOutQueue = new LinkedBlockingQueue<>();
                ReadThread readStd = new ReadThread(pStd, stdTimeOutQueue);
                readStd.start();

                String val = "";
                while(val != null)
                {
                    System.out.print(val);
                    val = stdTimeOutQueue.poll(1000, TimeUnit.MILLISECONDS);
                }
                readStd.stopRun();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
