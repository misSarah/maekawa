import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;

public class TCPMessage extends Thread {

    private Socket incomingSocket;
    private BlockingQueue<String> messageQueue;

    public TCPMessage(Socket incomingSocket, BlockingQueue<String> messageQueue) {
        this.incomingSocket = incomingSocket;
        this.messageQueue = messageQueue;
    }

    public void run() {
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(this.incomingSocket.getInputStream()));
            String incomingMessage = bufferedReader.readLine().trim();
            messageQueue.put(incomingMessage);
            bufferedReader.close();
            incomingSocket.close();
        } catch (IOException e) {
        } catch (InterruptedException e) {
        }
    }

}
