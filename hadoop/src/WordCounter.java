import java.io.FileInputStream;
import java.util.*;
import java.util.stream.Stream;

public class WordCounter {
    /**
     * counts the occurrences of words in the given scanner
     * @param scan the scanner on which to count the words
     * @return a HashMap mapping each word to its count
     */
    public static HashMap<String, Integer> count(Scanner scan){
        HashMap<String, Integer> wordCount = new HashMap<>();
        while (scan.hasNext()){
            String word = scan.next();
            if(wordCount.containsKey(word))
                wordCount.put(word, wordCount.get(word) +1);
            else
                wordCount.put(word, 1);
        }
        return sortByValue(sortByKey(wordCount));
    }

    /**
     * counts the number of occurrences of words in the given file
     * @param filename the name of the file on which to count the words
     * @return a HashMap mapping each word to its count
     */
    public static HashMap<String, Integer> count(String filename){
        try(Scanner file = new Scanner(new FileInputStream(filename))){
            return count(file);
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    // sort HashMap by Key
    private static HashMap<String, Integer> sortByKey(HashMap<String, Integer> map) {
        HashMap<String, Integer> ret = new LinkedHashMap<>();
        Stream<Map.Entry<String, Integer>> sequentialStream = map.entrySet().stream();
        sequentialStream.sorted(Map.Entry.comparingByKey()).forEachOrdered(c -> ret.put(c.getKey(), c.getValue()));
        return ret;
    }

    // sort HashMap by Value
    private static HashMap<String, Integer> sortByValue(HashMap<String, Integer> map) {
        HashMap<String, Integer> ret = new LinkedHashMap<>();
        Stream<Map.Entry<String, Integer>> sequentialStream = map.entrySet().stream();
        sequentialStream.sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEachOrdered(c -> ret.put(c.getKey(), c.getValue()));
        return ret;
    }
}

