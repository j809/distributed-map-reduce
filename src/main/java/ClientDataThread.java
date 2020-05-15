import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class ClientDataThread extends Thread {
    private final String address;
    private final int port;
    private Socket socket = null;

    public ClientDataThread(String address, int port) {
        this.address = address;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            socket = new Socket(address, port);
            Util.log("Connected to server");
            DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

            Util.log("Request file from server");
            out.write(WorkerCommand.FILE_REQUEST.toString().getBytes());
            out.flush();

            while(true) {
                byte[] readBytes = new byte[Util.BUFFER_SIZE];
                int count = in.read(readBytes);
                if(count == -1) {
                    Worker.setIsActive(false);
                    Util.log("Master data thread not found, closing worker data thread");
                    break;
                }
                String line = new String(readBytes, 0, count, StandardCharsets.UTF_8);
                Util.log("Master command: " + line);

                if ("TERMINATE".equals(line)) {
                    Util.log("Received terminate from server");
                    Worker.setIsActive(false);
                    out.write(WorkerCommand.TERMINATE_ACK.toString().getBytes());
                    out.flush();
                    break;

                } else {
                    String filePath = line.split(MasterCommand.HANDLE_FILE + ": ")[1];
                    Util.log("Received file from server: " + filePath);
                    Worker.processFile(filePath);
                    Util.log("File processed: " + filePath);

                    Util.log("Request file from server");

                    out.write(WorkerCommand.FILE_REQUEST.toString().getBytes());
                    out.flush();
                }
            }

            Util.log("Stopping client data thread");
            out.close();
            in.close();
            socket.close();
        } catch (IOException e) {
            Util.log(e.getMessage());
        }
    }
}