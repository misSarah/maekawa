import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Coordinator extends Thread {

    private BlockingQueue<String> incomingMessageQueue;

    private PriorityQueue<Request> requestsQueue =  new PriorityQueue<Request>();
    private Request currentRequest = null;

    public BlockingQueue<Message> applicationMessages = new LinkedBlockingQueue<Message>();
    private AtomicBoolean allRequestsFinished = new AtomicBoolean(false);
    public AtomicBoolean hasEnteredCriticalSection = new AtomicBoolean(false);

    public Coordinator(BlockingQueue<String> incomingMessageQueue) {
        this.incomingMessageQueue = incomingMessageQueue;
    }

    @Override
    public void run() {
        while (true) {
            try {

                if( this.allRequestsFinished.get() && this.incomingMessageQueue.isEmpty() && this.requestsQueue.isEmpty() && this.currentRequest == null ) {
                    return;
                }

                /*if( Node.verboseMode && this.allRequestsFinished.get() ) {
                    System.out.println("All Requests Finished but I did not exit because:");
                    System.out.printf("incomingMessageQueue = %d\n", this.incomingMessageQueue.size());
                    System.out.printf("requestsQueue = %d\n", this.requestsQueue.size());
                    System.out.printf("currentRequest = Node(%d)\n", this.currentRequest.getProcessId());
                }*/

                String rawMessage = this.incomingMessageQueue.poll(1, TimeUnit.SECONDS);

                if( rawMessage == null ) {
                    continue;
                }

                this.handleMessage(Message.parse(rawMessage));

            } catch (InterruptedException e) {}
        }
    }

    private void handleMessage(Message incomingMessage) {

        // 0. If the incoming message is null then it was not a known message type.
        if( incomingMessage == null ) {
            return;
        }

        // 1. Process messages that are from the verification protocol here (no vector clock)

        if ( incomingMessage.isa(Message.Type.FINISHED) ) {

            // 1. Mark as we are finished and it is now safe to close once closing condition is met.
            this.allRequestsFinished.set(true);

            // 2. Return; nothing more to do.
            return;

        }

        if( incomingMessage.isa(Message.Type.DEBUG) ) {

            // 1. If we are not Node 0 then we have no business processing this message.
            if( Node.nodeId != 0 ) {
                return;
            }

            // 2. Present this message to the verification process.
            try {
                VerificationHandler.incomingLogMessages.put((List<String>) incomingMessage.data);
            } catch (InterruptedException e) { }

            return;

        }

        synchronized (Node.Lock) {
            Node.clock.receivedTimestamp(incomingMessage.getSourceVectorClock());
        }

        if( incomingMessage.isa(Message.Type.REQUEST) ) {

            Request newRequest = incomingMessage.getRequest();
            //System.out.println(newRequest);

            // 1. If I have not already given the lock to someone then give the lock to this process.
            if( currentRequest == null ) {

                this.currentRequest = newRequest;
                Message.send(Message.Type.GRANT, this.currentRequest.getProcessId());

                return;

            }

            // 2. Otherwise I have given the lock to someone else.
            // If adding this request changes the head (in that that request will become the head), then
            // add the request to the queue and send a fail to everyone below the head. Send INQUIRE to
            // the current process if and only if the head request is less than the current request AND
            // we have not already sent an inquire to the current request.
            // If adding this request does not change the head, then it needs to be sent a failed because
            // we are waiting on other request ahead of it.
            if( requestChangesHead(newRequest) ) {

                Iterator<Request> requestIterator = this.requestsQueue.iterator();
                while(requestIterator.hasNext()) {

                    Request aRequest = requestIterator.next();

                    if( ! aRequest.haveSentFailed ) {
                        Message.send(Message.Type.FAILED, aRequest.getProcessId());
                    }

                }

                // The incoming request should be honored before the current request, attempt to INQUIRE
                if( newRequest.compareTo(this.currentRequest) == -1 ) {

                    if( ! this.currentRequest.haveSentInquire ) {
                        Message.send(Message.Type.INQUIRE, this.currentRequest.getProcessId());
                    }

                }

            } else {

                Message.send(Message.Type.FAILED, newRequest.getProcessId());
                newRequest.haveSentFailed = true;

            }

            // Add the request to the queue.
            this.requestsQueue.add(newRequest);

        }

        else if ( incomingMessage.isa(Message.Type.YIELD) ) {

            // 0. Only process a YIELD from the current request, because that is who I expect it from.
            if( incomingMessage.getSourceProcessId() != this.currentRequest.getProcessId() ) {
                return;
            }

            // 1. The current request has yielded to me, so we can add it to the queue.
            this.requestsQueue.add(this.currentRequest);

            // 2. Send a reply to the request at the top of the queue.
            this.currentRequest = this.requestsQueue.poll();

            if( this.currentRequest != null ) {
                Message.send(Message.Type.GRANT, this.currentRequest.getProcessId());
            }

        }

        else if ( incomingMessage.isa(Message.Type.INQUIRE) ) {

            // 1. If we have already entered the critical section then ignore the inquire.
            if( this.hasEnteredCriticalSection.get() == true ) {
                return;
            }

            // 2. Otherwise, expose the INQUIRE message to the
            try { this.applicationMessages.put(incomingMessage); } catch (InterruptedException e) {}

        }

        else if ( incomingMessage.isa(Message.Type.RELEASE) ) {

            // 1. Get the next request from the queue.
            this.currentRequest = this.requestsQueue.poll();

            // 2. If we have another request then send GRANT.
            if( this.currentRequest != null ) {
                Message.send(Message.Type.GRANT, this.currentRequest.getProcessId());
            }

        }

        else if ( incomingMessage.isa(Message.Type.FAILED) ) {

            // 1. Expose the failed message to the application.
            try { this.applicationMessages.put(incomingMessage); } catch (InterruptedException e) {}

        }

        else if ( incomingMessage.isa(Message.Type.GRANT) ) {

            // 1. Expose the grant message to the application.
            try { this.applicationMessages.put(incomingMessage); } catch (InterruptedException e) {}

        }

    }

    private boolean requestChangesHead(Request incomingRequest) {

        // Return true if queue is empty.
        if( this.requestsQueue.size() == 0 ) {
            return true;
        }

        Request currentHead = this.requestsQueue.peek();

        if( currentHead.compareTo(incomingRequest) == 1 ) {
            return true;
        }

        return false;

    }

}
