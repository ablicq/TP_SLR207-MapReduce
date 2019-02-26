public class Main {
    public static void main(String[] args) {
        int result = 3+2;
        try {Thread.sleep(10000);} catch (Exception e){e.printStackTrace();}
        System.out.println(result);
    }
}
