import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        HashMap<String, Integer> wordCount = WordCounter.count(args[0]);
        long endTime = System.currentTimeMillis();
        assert wordCount != null;
        // print first 50 elements (all the elements if there are less than 50)
        Iterator<Map.Entry<String, Integer>> it = wordCount.entrySet().iterator();
        int cpt = 0;
        while(cpt < 50 && it.hasNext()){
            Map.Entry<String, Integer> entry = it.next();
            System.out.println(entry.getKey() + " " + entry.getValue());
            cpt++;
        }
        long computationTime = endTime - startTime;
        System.out.println("\nComputation time: " + computationTime + "ms");
    }
}
