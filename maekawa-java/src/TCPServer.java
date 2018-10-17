import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.BlockingQueue;

public class TCPServer extends Thread {

    private InetSocketAddress serverAddress;
    private ServerSocket serverSocket;
    private BlockingQueue<String> messageQueue;

    public TCPServer(InetSocketAddress socketAddress, BlockingQueue<String> messageQueue) {
        this.serverAddress = socketAddress;
        this.messageQueue = messageQueue;
    }

    public void run() {

        try {

            this.serverSocket = new ServerSocket();
            this.serverSocket.bind(this.serverAddress);

            while (true) {
                new TCPMessage(serverSocket.accept(), this.messageQueue).start();
            }

        } catch (IOException exception) {
            return;
        } finally {
            this.closeSocket();
        }

    }

    public void closeSocket() {
        try {
            serverSocket.close();
        } catch (Exception e) { }
    }

}
