import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws IOException {
        long maxTimeMillis = 5000;
        ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/tmp/ablicq/SLAVE.jar");
        Process p = pb.start();

        BufferedInputStream pStd = new BufferedInputStream(p.getInputStream());
        LinkedBlockingQueue<String> stdTimeOutQueue = new LinkedBlockingQueue<>();
        ReadThread readStd = new ReadThread(pStd, stdTimeOutQueue);
        readStd.start();

        try {
            String val = stdTimeOutQueue.poll(maxTimeMillis, TimeUnit.MILLISECONDS);
            if(val == null){
                throw new NullPointerException();
            }
            System.out.println(val);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            p.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            readStd.stopRun();
        }
    }
}
