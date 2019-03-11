import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

public class Splitter {
    private ArrayList<String> hosts;

    public Splitter(ArrayList<String> hosts) {
        this.hosts = hosts;
    }

    public void deploy(){
        hosts.parallelStream().forEach((host) -> {

            ProcessBuilder mkdirPB = new ProcessBuilder(
                    "ssh",
                    "-o", "UserKnownHostsFile=/dev/null",
                    "-o", "StrictHostKeyChecking=no",
                    "ablicq@"+host,
                    "mkdir", "-p", "/tmp/ablicq/splits"
            );

            // get the list of files in /tmp/ablicq/splits to be able to copy these with scp
            File splitsFolder = new File("/tmp/ablicq/splits");
            File[] splitFiles = splitsFolder.listFiles();
            ArrayList<String> splits = new ArrayList<>();
            Arrays.stream(splitFiles).forEach(file -> splits.add("/tmp/ablicq/splits/"+file.getName()));

            ArrayList<String> copyCmd = new ArrayList<>();
            copyCmd.add("scp");
            copyCmd.addAll(splits);
            copyCmd.add("ablicq@"+host+":/tmp/ablicq/splits/");
            ProcessBuilder scpPB = new ProcessBuilder(
                    copyCmd
            );

            mkdirPB.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            mkdirPB.redirectError(ProcessBuilder.Redirect.INHERIT);

            scpPB.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            scpPB.redirectError(ProcessBuilder.Redirect.INHERIT);

            try {
                Process mkdir = mkdirPB.start();
                mkdir.waitFor();
                scpPB.start();
            } catch (IOException | InterruptedException e){
                e.printStackTrace();
            }
        });
    }

    public static void main(String[] args) {
        Splitter splitter = new Splitter(new ArrayList<>(Arrays.asList(args)));
        splitter.deploy();
    }
}
