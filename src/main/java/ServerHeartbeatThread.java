import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerHeartbeatThread extends Thread {

    private Socket socket;
    private ServerSocket serverSocket;

    public ServerHeartbeatThread(ServerSocket serverSocket){
        this.serverSocket = serverSocket;
    }

    @Override
    public void run() {
        try{
            socket = serverSocket.accept();
            socket.setSoTimeout(2000);
            Util.log("Heartbeat socket connection with client established");
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            boolean isTimeout = false;
            WordCount.setThreadMap(hashCode(), true);

            while(true) {
                if (!WordCount.getThreadMap(hashCode())) {
                    Util.log("Server data thread stopped, stopping heartbeat");
                    break;
                }
                if(isTimeout) {
                    WordCount.setThreadMap(hashCode(), false);
                    Util.log("OOPS! Worker inactive, must close server side data thread");
                    break;
                }
                out.writeUTF(MasterCommand.HEART_BEAT.toString());
                out.flush();
                try {
                    String read = in.readUTF();
                    Util.log("MSG FROM WORKER : " + read);
                    Thread.sleep(3000);
                } catch (Exception e) {
                    Util.log("Timeout occurred while receiving response for heartbeat");
                    isTimeout = true;
                }
            }

            Util.log("Server stopped heartbeats");
            in.close();
            out.close();
            socket.close();
        } catch (IOException e){
            Util.log(e.getMessage());
        }
    }
}
