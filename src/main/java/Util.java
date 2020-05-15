import java.util.*;

public class Util {
    public static final int BUFFER_SIZE = 4096 * 10;

    public static void log(String line) {
        System.out.println("[" + ProcessHandle.current().pid() + " : " + Thread.currentThread().getId() + "] : " + line + "\n");
    }

    public static Map<String, Long> arrangeMap(Map<String, Long> wordFrequency) {
        List<Map.Entry<String, Long>> entries = new ArrayList<>(wordFrequency.entrySet());
        Comparator<Map.Entry<String, Long>> comparator = (a, b) ->  {
            return !a.getValue().equals(b.getValue()) ?
                    Long.compare(a.getValue() * -1, b.getValue() * -1) :
                    a.getKey().compareTo(b.getKey());
        };
        Collections.sort(entries, comparator);

        wordFrequency.clear();
        for(Map.Entry<String, Long> entry : entries) {
            wordFrequency.put(entry.getKey(), entry.getValue());
        }

        return wordFrequency;
    }
}