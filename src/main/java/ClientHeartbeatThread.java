
import java.io.*;
import java.net.Socket;

public class ClientHeartbeatThread extends Thread {

    private final int serverPort;
    private final String ipAddress;
    private Socket socket = null;

    public ClientHeartbeatThread(String ipAddress, int serverPort){
        this.serverPort = serverPort;
        this.ipAddress = ipAddress;
    }

    @Override
    public void run() {
        try {
            socket = new Socket(ipAddress,serverPort);
            Util.log("Heartbeat socket connection with server established");
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            while(true){
                if (!Worker.getIsActive()) {
                    Util.log("Worker process inactive, so stopping worker side heartbeats");
                    break;
                }
                String read = in.readUTF();
                Util.log("Heart beat from server : " + read);
                out.writeUTF(WorkerCommand.ACK.toString() + " " + ProcessHandle.current().pid());
                out.flush();
            }
            in.close();
            out.close();
            socket.close();
        }catch (IOException e){
            Util.log(e.getMessage());
        }
    }
}
