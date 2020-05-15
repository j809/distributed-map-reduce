import java.io.*;
import java.util.*;

public class Worker {

    private final String ipAddress;
    private final int portA;
    private final int portB;
    private ClientDataThread clientDataThread;
    private ClientHeartbeatThread clientHeartbeatThread;
    private static Boolean isActive = true;

    public Worker(String ipAddress, int portA, int portB) {
        this.ipAddress = ipAddress;
        this.portA = portA;
        this.portB = portB;
    }

    public static Boolean getIsActive() {
        return isActive;
    }

    public static void setIsActive(Boolean activeStatus) {
        isActive = activeStatus;
    }

    public static void main(String[] args) throws Exception {
        String ipAddress = "127.0.0.1";
        Integer portA = Integer.parseInt("4000");
        Integer portB = Integer.parseInt("4500");
        Worker worker = new Worker(ipAddress, portA,portB);
        worker.run();
    }

    public void run() {
        clientDataThread = new ClientDataThread(ipAddress, portA);
        clientDataThread.start();
        clientHeartbeatThread = new ClientHeartbeatThread(ipAddress,portB);
        clientHeartbeatThread.start();
    }

    public static void processFile(String filePath) throws IOException {
        Map<String, Long> wordFrequency = new LinkedHashMap<>();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        BufferedWriter bw = new BufferedWriter(new FileWriter(filePath.replace("txt", "out")));
        try {
            String line = br.readLine();
            while (line != null) {
                if(!line.equals("")) {
                    List<String> words = Arrays.asList(line.split(" "));
                    for(String word: words) {
                        wordFrequency.put(word, wordFrequency.getOrDefault(word, 0L) + 1);
                    }
                }
                line = br.readLine();
            }

            Util.arrangeMap(wordFrequency);

            for(String word: wordFrequency.keySet()) {
                bw.write(wordFrequency.get(word) + " : " + word + System.lineSeparator());
            }
        } finally {
            br.close();
            bw.close();
        }
    }
}
