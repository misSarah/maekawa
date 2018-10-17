import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Node implements Runnable {

    public static boolean verboseMode = false;

    public static void main(String[] args) {

        if( args.length < 2 ) {
            System.out.println("Usage: Node <node id> <config file path> (--verbose)");
            System.exit(1);
        }

        Node.verboseMode = ( args.length >= 3 && args[2].equals("--verbose") );

        Node node = null;

        try {
            node = ConfigFile.process(Integer.parseInt(args[0]), ConfigFile.tokenize(args[1]));
        } catch (IOException e) {
            System.err.printf("Failed to parse config file: %s\nWill now exit!\n", e);
            System.exit(1);
        }

        if( node != null ) {

            if( Node.verboseMode ) {
                System.out.println(node);
            }

            node.run();

            System.out.flush();

        }

        System.exit(0);

    }

    public static int nodeId;

    public static HashMap<Integer, InetSocketAddress> nodeAddresses = new HashMap<Integer, InetSocketAddress>();
    public static HashMap<Integer, InetSocketAddress> quorumMembers = new HashMap<Integer, InetSocketAddress>();

    public static final Object Lock = new Object();
    public static VectorClock clock;

    private int numReqs;
    private ExponentialDelay delays;

    private BlockingQueue<String> messageQueue;
    private TCPServer tcpServer;
    private Coordinator coordinator;
    private Thread verificationHandler;

    public Node(int totalNodes, int numReqs, ExponentialDelay delays, InetSocketAddress socketAddress) {

        this.delays = delays;
        this.numReqs = numReqs;
        this.messageQueue = new LinkedBlockingQueue<String>();
        this.tcpServer = new TCPServer(socketAddress, this.messageQueue);
        this.coordinator = new Coordinator(this.messageQueue);

        clock = new VectorClock(totalNodes, Node.nodeId);

        if( Node.nodeId == 0 ) {
            this.verificationHandler = new Thread(new VerificationHandler(totalNodes * this.numReqs));
            this.verificationHandler.start();
        }

    }

    private void enterCS() {

        // 1. Send Request(ts,i) to all quorum members.
        Message.broadcast(Message.Type.REQUEST, Node.quorumMembers.values());

        // 2. Can only enter CS once a reply has been received from all quorum members.
        ArrayList<Integer> quorumReplies = new ArrayList<Integer>();

        while (true) {
            try {

                if( quorumReplies.size() == Node.quorumMembers.size() ) {
                    this.coordinator.hasEnteredCriticalSection.set(true);
                    this.coordinator.applicationMessages.clear();
                    break;
                }

                Message applicationMessage = this.coordinator.applicationMessages.poll(1, TimeUnit.SECONDS);

                if( applicationMessage != null ) {

                    // 2.1 If we received a GRANT from one of our quorum members then note it.
                    if( applicationMessage.isa(Message.Type.GRANT) && Node.quorumMembers.containsKey(applicationMessage.getSourceProcessId()) ) {
                        quorumReplies.add(new Integer(applicationMessage.getSourceProcessId()));
                    }

                    // 2.2 If we received a FAILED from one of our quorum members then remove the GRANT message they gave us.
                    if( applicationMessage.isa(Message.Type.FAILED) && Node.quorumMembers.containsKey(applicationMessage.getSourceProcessId()) ) {
                        quorumReplies.remove(new Integer(applicationMessage.getSourceProcessId()));
                    }

                    // 2.3 If we received an INQUIRE message from one of our quorum members then
                    if( applicationMessage.isa(Message.Type.INQUIRE) && Node.quorumMembers.containsKey(applicationMessage.getSourceProcessId()) ) {

                        // YIELD to that quorum member becuase we haven't received all of the required GRANT's
                        Message.send(Message.Type.YIELD, applicationMessage.getSourceProcessId());

                        Integer processId = applicationMessage.getSourceProcessId();

                        // Remove the GRANT (if any) from the node that sent us the INQUIRE.
                        if( quorumReplies.contains(processId) ) {
                            quorumReplies.remove(processId);
                        }

                    }

                }

            } catch (InterruptedException e) { }
        }

    }

    private int leaveCS() {

        int exitCSTime = 0;

        synchronized (Lock) {
            Node.clock.getTimestampForMessage();
            exitCSTime = Node.clock.getLogicalClockValue();
        }

        // 1. Send RELEASE(ts,i) to all quorum members.
        Message.broadcast(Message.Type.RELEASE, Node.quorumMembers.values());

        // 2. Set that we are no longer in the critical section.
        this.coordinator.hasEnteredCriticalSection.set(false);

        // 3. Return the leave logical clock time.
        return exitCSTime;

    }

    @Override
    public void run() {

        this.tcpServer.start();
        this.coordinator.start();

        for( int requestId = 1; requestId <= this.numReqs; requestId++ ) {

            this.enterCS();

            int enterCSTime = 0, exitCSTime = 0;

            // 2. Pretend to do some work in our CS.
            synchronized (Lock) {
                enterCSTime = clock.getLogicalClockValue();
            }

            try {
                Thread.sleep(this.delays.getCSDelay());
            } catch (InterruptedException e) {}




            // 3. We are done with our fake work, so leave the CS.
            exitCSTime = this.leaveCS();

            // 4. Send the status to the verification process running at Node(0)
            String debugMessage = String.format("DEBUG(%d,%d,%d,%d)", Node.nodeId, requestId, enterCSTime, exitCSTime);
            Message.send(debugMessage, Node.nodeAddresses.get(0));

            try {
                Thread.sleep(this.delays.getIRDelay());
            } catch (InterruptedException e) {}

        }

        try {
            coordinator.join();
        } catch (InterruptedException e) {}

        try {
            tcpServer.closeSocket();
            tcpServer.join();
        } catch (Exception e) { }

        if( this.verificationHandler != null ) {
            try {
                this.verificationHandler.join();
            } catch (InterruptedException e) {}
        }

    }

    @Override
    public String toString() {

        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(String.format("Node Id: %d\n", Node.nodeId));
        stringBuilder.append(String.format("Requests: %d\n", this.numReqs));
        stringBuilder.append(String.format("Delays: %s\n", this.delays));

        synchronized (Lock) {
            stringBuilder.append(String.format("Initial Vector Clock: %s\n", Node.clock));
        }

        stringBuilder.append("Neighboring Nodes:\n");
        Set<Integer> quorumMemberNodeIds = Node.quorumMembers.keySet();
        for( Map.Entry<Integer, InetSocketAddress> neighbor : Node.nodeAddresses.entrySet() ) {
            stringBuilder.append(String.format(" >> [%s] Node(%d) at %s\n", quorumMemberNodeIds.contains(neighbor.getKey()) ? "Q" : "-", neighbor.getKey(), neighbor.getValue() ));
        }

        return stringBuilder.toString();

    }


}
