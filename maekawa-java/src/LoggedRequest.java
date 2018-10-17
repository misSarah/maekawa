import java.util.Arrays;
import java.util.List;

public class LoggedRequest implements Comparable<LoggedRequest> {

    private int nodeId;
    private int requestId;
    private int startTime;
    private int endTime;
    private LoggedRequest conflict;

    public LoggedRequest(List<String> rawData) {
        this.nodeId = Integer.parseInt(rawData.get(0));
        this.requestId = Integer.parseInt(rawData.get(1));
        this.startTime = Integer.parseInt(rawData.get(2));
        this.endTime = Integer.parseInt(rawData.get(3));
        this.conflict = null;
    }

    @Override
    public int compareTo(LoggedRequest that) {

        if( this.startTime == that.startTime ) {
            return ( this.nodeId < that.nodeId ) ? -1 : 1;
        }

        return ( this.startTime < that.startTime ) ? -1 : 1;

    }

    public int getNodeId() {
        return this.nodeId;
    }

    public int getRequestId() {
        return this.requestId;
    }

    public boolean isConflicting() {
        return ( this.conflict != null );
    }

    public LoggedRequest getConflictingRequest() {
        return this.conflict;
    }

    public void setConflicting(LoggedRequest loggedRequest) {
        this.conflict = loggedRequest;
    }

    private static boolean ValInRange(int val, int a, int b) {
        return ( val > a && val < b );
    }

    public void checkIfConflictsWith(LoggedRequest otherRequest) {

        if( this.isConflicting() ) {
            return;
        }

        if(  ValInRange(otherRequest.startTime, this.startTime, this.endTime) || ValInRange(otherRequest.endTime, this.startTime, this.endTime) ) {
            this.conflict = otherRequest;
        }

    }

    @Override
    public String toString() {

        String descriptionCell = "";

        if( this.isConflicting() ) {
            descriptionCell = String.format("R(%d) @ Node(%d)", this.conflict.getRequestId(), this.conflict.getNodeId());
        }

        return String.format("<table cellpadding=\"2\" bgcolor=\"%s\"><tr><td>%d</td><td>%d</td></tr><tr><td colspan=\"2\">%s</td></tr></table>", this.isConflicting() ? "red" : "green", this.startTime, this.endTime, descriptionCell);

    }

}
