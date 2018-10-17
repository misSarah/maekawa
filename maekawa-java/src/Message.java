import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;

public class Message {

    public static enum Type { REQUEST, YIELD, INQUIRE, RELEASE, FAILED, GRANT, DEBUG, FINISHED }

    private Type type;
    private int sourceProcessId;
    private VectorClock sourceVectorClock;

    public Object data = null;

    public static void send(Type messageType, int processId) {

        InetSocketAddress nodeAddress = Node.nodeAddresses.get(new Integer(processId));

        if( nodeAddress == null ) {
            return;
        }

        send(messageType, nodeAddress);

    }

    public static void send(Type messageType, InetSocketAddress nodeAddress) {

        String message = null;

        synchronized (Node.Lock) {
            message = String.format("%s(%s,%d)", messageType.name(), Node.clock.getTimestampForMessage(), Node.nodeId);
        }

        send(message, nodeAddress);

    }

    public static void broadcast(Type messageType, Collection<InetSocketAddress> nodeAddresses) {

        String message = null;

        synchronized (Node.Lock) {
            message = String.format("%s(%s,%d)", messageType.name(), Node.clock.getTimestampForMessage(), Node.nodeId);
        }

        for( InetSocketAddress nodeAddress : nodeAddresses ) {
            send(message, nodeAddress);
        }

    }

    public static void send(String data, InetSocketAddress address) {
        send(data, address, true);
    }

    public static void send(String data, InetSocketAddress address, boolean retry) {

        if( address == null ) {
            return;
        }

        while(true) {
            try {
                Socket serverSocket = new Socket();
                serverSocket.connect(address);
                PrintWriter printWriter = new PrintWriter(serverSocket.getOutputStream(), true);
                printWriter.println(data);
                printWriter.close();
                serverSocket.close();
                return;
            } catch(Exception exception) {

                //System.err.printf("Failed to send '%s' to %s and retry is set to %b.\n", data, address, retry);

                if( ! retry ) {
                    return;
                }

                try { Thread.sleep(1000); } catch (InterruptedException e) {}
                continue;
            }
        }

    }

    public static Message parse(String messageData) {

        if( messageData == null || messageData.length() == 0 ) {
            return null;
        }

        // Incoming message format is NAME(Args, ...) we will split "NAME" and "Args, ..."
        String[] messageComponents = messageData.substring(0, messageData.length() - 1).split("\\(");

        String messageType = messageComponents[0];
        String[] messageArguments = messageComponents[1].split(",");

        Message message = null;

        // REQUEST(ts,i)
        if( messageType.equals("REQUEST") ) {
            message = new Message(Integer.parseInt(messageArguments[1]), new VectorClock(messageArguments[0]));
            message.type = Type.REQUEST;
        }

        // YIELD(ts,i)
        else if( messageType.equals("YIELD") ) {
            message = new Message(Integer.parseInt(messageArguments[1]), new VectorClock(messageArguments[0]));
            message.type = Type.YIELD;
        }

        // INQUIRE(ts,i)
        else if( messageType.equals("INQUIRE") ) {
            message = new Message(Integer.parseInt(messageArguments[1]), new VectorClock(messageArguments[0]));
            message.type = Type.INQUIRE;
        }

        // RELEASE(ts,i)
        else if( messageType.equals("RELEASE") ) {
            message = new Message(Integer.parseInt(messageArguments[1]), new VectorClock(messageArguments[0]));
            message.type = Type.RELEASE;
        }

        // FAILED(ts,i)
        else if( messageType.equals("FAILED") ) {
            message = new Message(Integer.parseInt(messageArguments[1]), new VectorClock(messageArguments[0]));
            message.type = Type.FAILED;
        }

        // GRANT(ts,i)
        else if( messageType.equals("GRANT") ) {
            message = new Message(Integer.parseInt(messageArguments[1]), new VectorClock(messageArguments[0]));
            message.type = Type.GRANT;
        }

        // DEBUG(nodeId, requestId, startTime, endTime)
        else if( messageType.equals("DEBUG") ) {
            message = new Message(-1, null);
            message.type = Type.DEBUG;
            message.data = (Object) Arrays.asList(messageArguments);
        }

        // FINISHED()
        else if( messageType.equals("FINISHED")  ) {
            message = new Message(-1, null);
            message.type = Type.FINISHED;
        }

        return message;

    }

    public Message(int sourceProcessId, VectorClock sourceVectorClock) {
        this.sourceProcessId = sourceProcessId;
        this.sourceVectorClock = sourceVectorClock;
    }

    public boolean isa(Message.Type type) {
        return ( this.type == type );
    }

    public int getSourceProcessId() {
        return this.sourceProcessId;
    }

    public VectorClock getSourceVectorClock() {
        return this.sourceVectorClock;
    }

    public Request getRequest() {

        if( ! this.isa(Type.REQUEST) ) {
            return null;
        }

        return new Request(this);

    }

}
