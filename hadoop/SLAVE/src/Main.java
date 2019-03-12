public class Main {
    public static void main(String[] args) {
        // check the validity of the args format
        if(args.length != 2){
            System.err.println("Error: Wrong Arguments");
            System.err.println("Usage: java -jar slave.jar <mode> <inputFile>");
            System.exit(-1);
        }

        // extract information from args
        int mode = Integer.parseInt(args[0]);
        String inFile = args[1];

        // apply the desired function to the input file
        switch (mode){
            case 0:
                Mapper.map(inFile);
        }
    }
}
