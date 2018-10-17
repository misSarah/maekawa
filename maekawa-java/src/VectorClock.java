import java.util.Arrays;

public class VectorClock {

    private int[] vector;
    private int ownerProcessId;

    public VectorClock(int numProcesses, int ownerProcessId) {
        this.ownerProcessId = ownerProcessId;
        this.vector = new int[numProcesses];
        this.vector[ownerProcessId] = 1;
    }

    public VectorClock(int[] aVector, int ownerProcessId) {
        this.ownerProcessId = ownerProcessId;
        this.vector = new int[aVector.length];
        for( int i = 0; i < aVector.length; i++ ) {
            this.vector[i] = aVector[i];
        }
    }

    public VectorClock(String vectorClockData) {

        String[] split = vectorClockData.split("\\[");
        String[] vectorValues = split[1].substring(0, split[1].length() - 1).split(":");

        this.ownerProcessId = Integer.parseInt(split[0]);
        this.vector = new int[vectorValues.length];

        for( int i = 0; i < this.vector.length; i++ ) {
            this.vector[i] = Integer.parseInt(vectorValues[i]);
        }

    }

    public int getOwnerProcessId() {
        return this.ownerProcessId;
    }

    public String getTimestampForMessage() {
        this.vector[this.ownerProcessId]++;
        return this.toString();
    }

    public void receivedTimestamp(String vectorClockData) {
        this.receivedTimestamp(new VectorClock(vectorClockData));
    }

    public void receivedTimestamp(VectorClock otherVectorClock) {

        for( int i = 0; i < this.vector.length; i++ ) {
            this.vector[i] = Math.max(this.vector[i], otherVectorClock.vector[i]);
        }
        this.vector[this.ownerProcessId]++;
    }

    public boolean happenedBefore(VectorClock that) {

        if( that == null ) {
            return false;
        }

        boolean atLeastOneLessThan = false;

        for( int i = 0; i < this.vector.length; i++ ) {

            int val1 = this.vector[i];
            int val2 = that.vector[i];

            if( this.vector[i] > that.vector[i] ) {
                return false; // all this <= that
            }

            if( this.vector[i] < that.vector[i] ) {
                atLeastOneLessThan = true;
            }

        }

        return atLeastOneLessThan;

    }

    public int getLogicalClockValue() {

        int sum = 0;

        for( int val : this.vector ) {
            sum += val;
        }

        return sum;

    }

    @Override
    public String toString() {
        String out = String.format("%d[", this.getOwnerProcessId());
        for( int val : vector ) {
            out += String.format("%d:", val);
        }
        return out.substring(0, out.length() - 1) + "]";
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VectorClock that = (VectorClock) o;

        if (this.ownerProcessId != that.ownerProcessId) {
            return false;
        }

        return Arrays.equals(this.vector, that.vector);

    }

    public static void main(String[] args) {

        {
            System.out.println("===BEGIN TEST 1===");

            VectorClock vectorClock1 = new VectorClock(3, 1);
            vectorClock1.getTimestampForMessage();
            System.out.printf("Vector Clock 1: %s\n", vectorClock1);

            VectorClock vectorClock2 = new VectorClock(vectorClock1.toString());
            System.out.printf("Vector Clock 2: %s\n", vectorClock2);

            System.out.printf("1 & 2 Equal? %b\n", vectorClock2.equals(vectorClock1));

            System.out.println("===END TEST 1===");
        }

        {
            System.out.println("===BEGIN TEST 2===");

            VectorClock p1Clock = new VectorClock(3, 1);
            VectorClock p2Clock = new VectorClock(3, 2);

            System.out.printf("P1's Clock: %s\n", p1Clock);
            System.out.printf("P2's Clock: %s\n", p2Clock);

            System.out.println("<P1 will send a message to P2.>");
            String p1Top2Timestamp = p1Clock.getTimestampForMessage();

            System.out.printf("P1's Clock after sending message: %s\n", p1Clock);

            p2Clock.receivedTimestamp(p1Top2Timestamp);

            System.out.printf("P2's Clock after receiving the message: %s\n", p2Clock);


            System.out.printf("P1 happened before P2 (Yes): %b\n", p1Clock.happenedBefore(p2Clock));
            System.out.printf("P2 happened before P1 (No): %b\n", p2Clock.happenedBefore(p1Clock));

            System.out.println("===END TEST 2===");
        }

        {
            System.out.println("===BEGIN TEST 3===");

            VectorClock vc1 = new VectorClock(new int[]{1, 2, 0}, 1);
            VectorClock vc2 = new VectorClock(new int[]{2,3,1}, 1);

            System.out.printf("vc1=%s\nvc2=%s\n", vc1, vc2);
            System.out.printf("vc1 -> vc2 (yes): %b\n", vc1.happenedBefore(vc2));
            System.out.printf("vc2 -> vc1 (no): %b\n\n", vc2.happenedBefore(vc1));

            VectorClock vc3 = new VectorClock(new int[]{1, 2, 1}, 1);
            VectorClock vc4 = new VectorClock(new int[]{2, 1, 3}, 1);

            System.out.printf("vc3=%s\nvc4=%s\n", vc3, vc4);
            System.out.printf("vc3 -> vc4 (no): %b\n", vc3.happenedBefore(vc4));
            System.out.printf("vc4 -> vc3 (no): %b\n\n", vc4.happenedBefore(vc4));

            VectorClock vc5 = new VectorClock(new int[]{3, 3, 4}, 1);
            VectorClock vc6 = new VectorClock(new int[]{3, 3, 4}, 1);

            System.out.printf("vc5=%s\nvc6=%s\n", vc5, vc6);
            System.out.printf("vc5 -> vc6 (no): %b\n", vc5.happenedBefore(vc6));
            System.out.printf("vc6 -> vc5 (no): %b\n", vc6.happenedBefore(vc5));

            System.out.println("===END TEST 3===");
        }

        {
            System.out.println("===BEGIN TEST 4===");

            VectorClock vc1 = new VectorClock(new int[]{1, 0, 0}, 1);
            VectorClock vc2 = new VectorClock(new int[]{0, 1, 0}, 1);

            System.out.printf("vc1=%s\nvc2=%s\n", vc1, vc2);
            System.out.printf("vc1 -> vc2 (no) : %b\n", vc1.happenedBefore(vc2));
            System.out.printf("vc2 -> vc1 (no) : %b\n", vc2.happenedBefore(vc1));

            System.out.println("===END TEST 4===");
        }

    }

}
