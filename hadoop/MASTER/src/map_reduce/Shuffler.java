package map_reduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

public class Shuffler {

    private ArrayList<String> hosts;
    private HashMap<String, ArrayList<Integer>> keySplitMap;
    private HashMap<String, ArrayList<Integer>> splitAssignments;

    private HashMap<String, ArrayList<String>> mapAssignments = new HashMap<>();
    private HashMap<String, HashSet<Integer>> filesToTransfer = new HashMap<>();


    public Shuffler(ArrayList<String> hosts, HashMap<String, ArrayList<Integer>> keySplitMap, HashMap<String, ArrayList<Integer>> splitAssignments) {
        this.hosts = hosts;
        this.keySplitMap = keySplitMap;
        this.splitAssignments = splitAssignments;
    }

    public HashMap<String, ArrayList<String>> getMapAssignments() {
        return mapAssignments;
    }

    public HashMap<String, HashSet<Integer>> getFilesToTransfer() {
        return filesToTransfer;
    }


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
}
