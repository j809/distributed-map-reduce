import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Queue;

public class ServerDataThread extends Thread {
    private Socket socket;
    private ServerSocket server;
    private Queue<String> files;
    private int heartBeatThreadHashCode;

    public ServerDataThread(ServerSocket server, Queue<String> files, int heartBeatThreadHashCode) {
        this.server = server;
        this.files = files;
        this.heartBeatThreadHashCode = heartBeatThreadHashCode;
    }

    @Override
    public void run() {
        try
        {
            socket = server.accept();
            Util.log("Client accepted");
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            boolean waitingForTerminateAck = false;
            while(WordCount.getThreadMap(heartBeatThreadHashCode)) {
                if(waitingForTerminateAck) {
                    WordCount.setThreadMap(heartBeatThreadHashCode, false);
                    break;
                }

                byte[] readBytes = new byte[Util.BUFFER_SIZE];
                int count = in.read(readBytes);
                if(count == -1)
                    break;
                String line = new String(readBytes, 0, count, StandardCharsets.UTF_8);
                Util.log("Worker request: " + line);

                if("FILE_REQUEST".equals(line)) {
                    String fileName;
                    while(true) {
                        fileName = files.poll();
                        if(fileName == null || !WordCount.getFileStatus(fileName))
                            break;
                    }
                    boolean sendFile = true;
                    if (fileName == null)
                    {
                        Util.log("No more files queued, terminating worker");
                        out.write(MasterCommand.TERMINATE.toString().getBytes());
                        out.flush();
                        sendFile = false;
                    }

                    if (sendFile) {
                        Util.log("Sending file to worker: " + fileName);
                        out.write((MasterCommand.HANDLE_FILE + ": " + fileName).getBytes());
                        out.flush();

                        while(in.available() == 0 && WordCount.getThreadMap(heartBeatThreadHashCode));

                        if(!WordCount.getThreadMap(heartBeatThreadHashCode)) {
                            Util.log("Worker died, queuing back file: " + heartBeatThreadHashCode + " " +  WordCount.getThreadMap(heartBeatThreadHashCode));
                            WordCount.setThreadMap(heartBeatThreadHashCode, false);
                            files.add(fileName);
                            WordCount.wordCount.spawnWorkerThroughThread();
                            break;
                        }
                        else {
                            WordCount.setFileStatus(fileName, true);
                        }
                    }
                    else {
                        Util.log("Waiting for terminate ACK from client");
                        waitingForTerminateAck = true;
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                else if("TERMINATE_ACK".equals(line)) {
                    Util.log("Received terminate ACK from client");
                    break;
                }
                else {
                    Util.log("Invalid worker command: " + line);
                }
            }

            Util.log("Server data thread stopped");
            in.close();
            out.close();
            socket.close();
        }
        catch(IOException e)
        {
            Util.log(e.getMessage());
        }
    }
}