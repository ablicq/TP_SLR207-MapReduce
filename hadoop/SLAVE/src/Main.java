import java.util.ArrayList;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        // extract mode from args
        int mode = Integer.parseInt(args[0]);

        // apply the desired function
        switch (mode){
            case 0: // splits to unsorted maps
                String inFile = args[1];
                Mapper.map(inFile);
                break;
            case 1: // unsorted maps to sorted maps
                String mapKey = args[1];
                String outNo = args[2];
                ArrayList<String> maps = new ArrayList<>(Arrays.asList(args).subList(2, args.length -1));
                Mapper.sortMaps(mapKey, outNo, maps);
                break;
            case 2: // sorted maps to reduced maps
                String reduceKey = args[1];
                String inNo = args[2];
                Reducer.reduce(reduceKey, inNo);
                break;
        }
    }
}
