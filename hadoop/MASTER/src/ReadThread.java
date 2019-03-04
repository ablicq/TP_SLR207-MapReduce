import java.io.BufferedInputStream;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * read the input from the given input stream and relays the data to the given queue
 */
public class ReadThread extends Thread {

    /**
     * the InputStream from which to read data
     */
    private BufferedInputStream in;

    /**
     * the queue on which to put data
     */
    private LinkedBlockingQueue<String> queue;

    /**
     * a boolean to stop the thread when wanted
     */
    private boolean isRunning = true;

    /**
     * the constructor of the thread
     * @param in the InputStream from which to read data
     * @param queue the queue on which to put data
     */
    public ReadThread(BufferedInputStream in, LinkedBlockingQueue<String> queue) {
        this.in = in;
        this.queue = queue;
    }

    /**
     * read the input from the given input stream and relays the data to the given queue
     */
    @Override
    public void run() {
        while (isRunning) {
            try {
                if (in.available() > 0) {
                    // parse the input as a character and not an integer
                    char read = (char) in.read();
                    queue.put(Character.toString(read));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * sets the isRunning variable to false to stop the thread
     */
    public synchronized void stopRun(){
        isRunning = false;
    }
}
