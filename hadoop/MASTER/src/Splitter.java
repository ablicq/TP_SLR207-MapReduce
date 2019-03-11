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

            ProcessBuilder scpPB = new ProcessBuilder(
                    "scp", "/tmp/ablicq/splits/*", "ablicq@"+host+":/tmp/ablicq/splits/"
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
