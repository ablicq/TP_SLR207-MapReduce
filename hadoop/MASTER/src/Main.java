import map_reduce.JobsManager;

import java.util.HashMap;

public class Main {
    public static void main(String[] args) {
        JobsManager jobsManager = new JobsManager("../slaves.conf", 3);
        jobsManager.runAll();
        HashMap<String, Integer> wordCount = jobsManager.fetchResults();
        wordCount.forEach((word, count) -> System.out.println(word + ": " +count));
    }
}
