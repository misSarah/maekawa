import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class VerificationHandler implements Runnable {

    private int expectedRequests = 0; // The expected number of requests to be made by the whole system. e.g. 5 reqs * 5 nodes = 25 reqs
    private int completedRequests = 0; // The number of requests have have been completed by all nodes so far.
    private int maximumRequests = 0; // The maximum number of requests a single node has made.
    private int nodeCount = 0; // The number of nodes talking to this system.

    public static BlockingQueue<List<String>> incomingLogMessages = new LinkedBlockingQueue<List<String>>();

    private PriorityQueue<LoggedRequest> loggedRequestsQueue;
    private HashMap<Integer, ArrayList<LoggedRequest>> nodesRequests;

    public VerificationHandler(int expectedRequests) {
        this.expectedRequests = expectedRequests;
        this.loggedRequestsQueue = new PriorityQueue<LoggedRequest>();
        this.nodesRequests = new HashMap<Integer, ArrayList<LoggedRequest>>();
    }

    private void recordLoggedRequest(LoggedRequest loggedRequest) {

        if( ! this.nodesRequests.containsKey(loggedRequest.getNodeId()) ) {
            this.nodesRequests.put(loggedRequest.getNodeId(), new ArrayList<LoggedRequest>());
        }

        this.nodesRequests.get(loggedRequest.getNodeId()).add(loggedRequest);
        this.loggedRequestsQueue.add(loggedRequest);

    }

    private void validateRequestsQueue() {

        // 1. If we don't have at least 2 requests then we have nothing to validate.
        if( this.loggedRequestsQueue.size() < 2 ) {
            return;
        }

        // 2. Iterate through the requests maintaining pointers to prev and next.
        Iterator<LoggedRequest> loggedRequestIterator = this.loggedRequestsQueue.iterator();
        LoggedRequest previous = null, next = null;

        while( loggedRequestIterator.hasNext() ) {

            LoggedRequest current = loggedRequestIterator.next();

            if( loggedRequestIterator.hasNext() ) {
                next = loggedRequestIterator.next();
            }

            if( previous == null && next == null ) {
                continue; // we have nothing to validate against.
            }

            if( previous != null ) {
                current.checkIfConflictsWith(previous);
            }

            if( next != null ) {
                current.checkIfConflictsWith(next);
            }

            previous = current;

        }

    }

    private void makeValidationTable() {

        StringBuilder nodeColumns = new StringBuilder();
        StringBuilder[] requestRows = new StringBuilder[this.maximumRequests];

        for( int i = 0; i < requestRows.length; i++ ) {
            requestRows[i] = new StringBuilder(String.format("<tr><td><strong>Request %d</strong></td>", i+1));
        }

        for( Map.Entry<Integer, ArrayList<LoggedRequest>> nodeRequests : this.nodesRequests.entrySet() ) {

            nodeColumns.append(String.format("<td>Node %d</th>", nodeRequests.getKey()));

            for( LoggedRequest request : nodeRequests.getValue() ) {

                requestRows[Math.max(0, request.getRequestId() - 1)].append(String.format("<td>%s</td>", request));

            }

        }

        StringBuilder combinedRequestsRows = new StringBuilder();

        for( int i = 0; i < requestRows.length; i++ ) {
            requestRows[i].append("</tr>");
            combinedRequestsRows.append(requestRows[i].toString());
        }

        String tableTitle = String.format("<h3>%d of %d Requests (%d%%)</h3><hr />", this.completedRequests, this.expectedRequests, (int) ((this.completedRequests / (double)this.expectedRequests) * 100));
        String validationTable = String.format("%s<table cellpadding=\"5\"><thead><tr><th></th>%s</tr></thead><tbody>%s</tbody></table>", tableTitle, nodeColumns.toString(), combinedRequestsRows.toString());

        Writer writer = null;

        try {
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("verification-table.html"), "utf-8"));
            writer.write(validationTable);
        } catch (IOException ex) {
            System.out.println(validationTable);
        } finally {
            try {writer.close();} catch (Exception ex) {}
        }

        validationTable = null;
        tableTitle = null;
        nodeColumns = null;
        requestRows = null;

        System.gc();

    }

    @Override
    public void run() {

        // 1. Wait to receive all requests from all nodes and we have no more requests.
        while ( this.completedRequests < this.expectedRequests ) {
            try {

                List<String> logMessage = this.incomingLogMessages.poll(5, TimeUnit.SECONDS);

                if( logMessage == null ) {
                    continue;
                }

                LoggedRequest loggedRequest = new LoggedRequest(logMessage);
                System.gc();

                this.recordLoggedRequest(loggedRequest);

                this.maximumRequests = Math.max(this.maximumRequests, loggedRequest.getRequestId());
                this.nodeCount = Math.max(this.nodeCount, loggedRequest.getNodeId());
                this.completedRequests++;

            } catch (InterruptedException e) {}
        }

        // 2. Validate the requests queue one last time and finalize the output table.
        this.validateRequestsQueue();
        this.makeValidationTable();

        // 3. Send done messages to all nodes.
        for( InetSocketAddress nodeAddress : Node.nodeAddresses.values() ) {
            while(true) {
                try {
                    Socket serverSocket = new Socket();
                    serverSocket.connect(nodeAddress);
                    PrintWriter printWriter = new PrintWriter(serverSocket.getOutputStream(), true);
                    printWriter.println("FINISHED(-1)");
                    printWriter.close();
                    serverSocket.close();
                    break;
                } catch(Exception exception) {
                    try { Thread.sleep(1000); } catch (InterruptedException e) {}
                    continue;
                }
            }
        }

    }

}
