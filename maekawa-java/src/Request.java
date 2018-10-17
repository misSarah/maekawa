public class Request implements Comparable<Request> {

    private Message requestMessage;

    public boolean haveSentInquire = false, haveSentFailed = false;

    public Request(Message requestMessage) {
        this.requestMessage = requestMessage;
    }

    public int getProcessId() {
        return this.requestMessage.getSourceProcessId();
    }

    @Override
    public int compareTo(Request that) {

        int myLogicalClock = this.requestMessage.getSourceVectorClock().getLogicalClockValue();
        int thatLogicalClock = that.requestMessage.getSourceVectorClock().getLogicalClockValue();

        if( myLogicalClock < thatLogicalClock ) {
            return -1;
        }

        if( myLogicalClock > thatLogicalClock ) {
            return 1;
        }

        if( this.requestMessage.getSourceProcessId() < that.requestMessage.getSourceProcessId() ) {
            return -1;
        }

        if( this.requestMessage.getSourceProcessId() > that.requestMessage.getSourceProcessId() ) {
            return 1;
        }

        // We should never reach this point.
        return 0;

    }

}