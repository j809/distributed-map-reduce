import java.io.*;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;


public class WordCount implements Master {
    private final int workerNum;
    private final String[] filenames;
    private ServerSocket socketServerA;
    private ServerSocket socketServerB;
    private int serverPortA;
    private int serverPortB;
    private Collection<Process> activeProcesses;
    private Queue<String> todoFiles;
    private List<String> fileList;
    private PrintStream outputStream;
    private static Map<Integer, Boolean> threadMap = new ConcurrentHashMap<>();
    private static Map<String, Boolean> fileCompletionMap = new ConcurrentHashMap<>();
    public static ExecutorService executor;
    public static WordCount wordCount;

    public WordCount(int workerNum, String[] filenames) throws IOException {
        setSingularWordCount(this);
        this.workerNum = workerNum;
        this.filenames = filenames;
        this.activeProcesses = new LinkedList<>();
        executor = Executors.newFixedThreadPool(workerNum);
    }

    public static Boolean getThreadMap(Integer threadHashCode) {
        return threadMap.get(threadHashCode);
    }

    public static void setThreadMap(Integer threadHashCode, Boolean status) {
        threadMap.put(threadHashCode, status);
    }

    public static Boolean getFileStatus(String file) {
        return fileCompletionMap.get(file);
    }

    public static void setFileStatus(String file, Boolean status) {
        fileCompletionMap.put(file, status);
    }

    public static void setSingularWordCount(WordCount wc) {
        wordCount = wc;
    }

    public static void main(String[] args) throws Exception {
        String directoryPath = parseArgs(args);
        File directory = new File(directoryPath);

        File[] files = directory.listFiles((d, name) -> name.endsWith(".txt"));
        String[] fileNames = new String[files.length];
        int c = 0;
        for(File file : files) {
            fileNames[c++] = file.getPath();
        }

        int numWorkers = Integer.parseInt(args[0].trim());
        String finalOutput = args[2].trim();

        WordCount wordCount = new WordCount(numWorkers, fileNames);
        PrintStream outputStream = new PrintStream(new FileOutputStream(finalOutput));
        wordCount.setOutputStream(outputStream);
        wordCount.run();
    }

    private static String parseArgs(String[] args) {
        // TODO: WORKERS SHOULD NOT STAY IDLE SO IF NUMBER OF INPUT FILES IS LESS THAN THE WORKERS WE SHOULD TAKE CARE OF THIS SCENARIO
        return args[1].trim();
    }

    public void setOutputStream(PrintStream out) {
        this.outputStream = out;
    }

    public void spawnWorkerThroughThread() {
        executor.execute(() -> {
            try {
                Util.log("trying to start worker");
                createWorker();
            } catch (IOException e) {
                Util.log("Error creating worker!");
            }
        });
    }

    public void run() {
        todoFiles = new ConcurrentLinkedQueue<>();
        fileList = new ArrayList<>();
        for(String file : filenames) {
            fileCompletionMap.put(file, false);
            todoFiles.add(file);
            fileList.add(file.replace("txt", "out"));
        }

        serverPortA = 4000;
        serverPortB = 4500;
        try {
            socketServerA = new ServerSocket(serverPortA);
            Util.log("Server initialized on port " + serverPortA);
        } catch (IOException e) {
            Util.log("Error initializing server on port " + serverPortA);
        }

        try {
            socketServerB = new ServerSocket(serverPortB);
            Util.log("Server Heartbeat thread initialized on port " + serverPortB);
        } catch (IOException e) {
            Util.log("Error initializing Heartbeat server on port " + serverPortB);
        }

        for(int i=0; i<workerNum; ++i){
            spawnWorkerThroughThread();
        }

        while(true) {
            boolean allFilesCompleted = true;
            for(Map.Entry<String, Boolean> entry : fileCompletionMap.entrySet()) {
                if(Boolean.FALSE.equals(entry.getValue())) {
                    allFilesCompleted = false;
                    break;
                }
            }

            if (allFilesCompleted) {
                break;
            }
        }

        executor.shutdown();
        while (true) {
            if(executor.isTerminated())
                break;
        }

        try {
            socketServerA.close();
            socketServerB.close();
        } catch (IOException e) {
            Util.log("Exception while closing server sockets");
        }

        Util.log("All file processed. Starting aggregation.");

        try {
            aggregateFiles(this.outputStream);
        } catch (IOException e) {
            Util.log("Exception while aggregating files");
        }

        if(outputStream != null) {
            outputStream.close();
        }
    }

    private void aggregateFiles(PrintStream outputStream) throws IOException {
        if (outputStream == null)
            return;

        Map<String, Long> wordFrequency = new LinkedHashMap<>();
        for(String file : fileList) {
            BufferedReader br = new BufferedReader(new FileReader(file));
            String line = br.readLine();
            while (line != null) {
                List<String> parts = Arrays.stream(line.split(":")).map(String::trim).collect(Collectors.toList());
                wordFrequency.put(parts.get(1), wordFrequency.getOrDefault(parts.get(1), 0L) + Long.parseLong(parts.get(0)));
                line = br.readLine();
            }
        }

        Util.arrangeMap(wordFrequency);


        BufferedOutputStream bw = new BufferedOutputStream(outputStream);
        for (String word : wordFrequency.keySet()) {
            String line = wordFrequency.get(word) + " : " + word + System.lineSeparator();
            bw.write(line.getBytes());
        }
        bw.close();

        Util.log("Final output file produced");
    }

    public Collection<Process> getActiveProcess() {
        return activeProcesses;
    }

    public void createWorker() throws IOException {
        ServerHeartbeatThread serverHeartbeatThread = new ServerHeartbeatThread(socketServerB);
        ServerDataThread serverDataThread = new ServerDataThread(socketServerA, todoFiles, serverHeartbeatThread.hashCode());
        setThreadMap(serverHeartbeatThread.hashCode(), true);
        serverHeartbeatThread.start();
        serverDataThread.start();

        Process forkedProcess = null;
        try {
            JavaProcess javaProcess = new JavaProcess(Worker.class);
            forkedProcess = javaProcess.exec();
            activeProcesses.add(forkedProcess);
            forkedProcess.waitFor();
            activeProcesses.remove(forkedProcess);
            Util.log("Process exited with exit value: " + forkedProcess.exitValue());
        } catch (InterruptedException e) {
            Util.log("Thread interrupted externally");
        } finally {
            if (forkedProcess != null) {
                forkedProcess.destroy();
            }
        }
    }
}

