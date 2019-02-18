import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/tmp/ablicq/SLAVE.jar");
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        pb.start();
    }
}
