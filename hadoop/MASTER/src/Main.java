import map_reduce.JobsManager;
import map_reduce.Splitter;

import java.util.HashMap;

public class Main {
    public static void main(String[] args) {
        int nbSplits= Splitter.split(args[0], args[1]);
        System.out.println("split phase finished with "+nbSplits+" splits");
        JobsManager jobsManager = new JobsManager("../slaves.conf", nbSplits);
        jobsManager.runAll();
        HashMap<String, Integer> wordCount = jobsManager.fetchResults();
        wordCount.forEach((word, count) -> System.out.println(word + ": " +count));
    }
}
