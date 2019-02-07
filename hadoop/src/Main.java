import java.util.HashMap;

public class Main {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        HashMap<String, Integer> wordCount = WordCounter.count(args[0]);
        long endTime = System.currentTimeMillis();
        assert wordCount != null;
        wordCount.forEach((key, value) -> System.out.println(key + " " + value));
        long computationTime = endTime - startTime;
        System.out.println("\nComputation time: " + computationTime + "ms");
    }
}
