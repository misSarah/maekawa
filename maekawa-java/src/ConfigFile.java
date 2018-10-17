import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ConfigFile {

    public static ArrayList<String[]> tokenize(String configFilePath) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(configFilePath));
        ArrayList<String[]> tokenizedLines = new ArrayList<String[]>();

        while(reader.ready()) {

            String line = reader.readLine().trim();

            if ( line.length() == 0 || line.charAt(0) == '#' ) {
                continue;
            }

            tokenizedLines.add(line.split("\\s+"));

        }

        reader.close();

        return tokenizedLines;

    }

    public static Node process(int nodeId, ArrayList<String[]> tokenizedLines) {

        // 1. Set the node id globally
        Node.nodeId = nodeId;

        // 2. Create the exponential delay object
        ExponentialDelay exponentialDelay = new ExponentialDelay(Integer.parseInt(tokenizedLines.get(0)[1]), Integer.parseInt(tokenizedLines.get(0)[2]));

        // 3. Create the socket address for this node
        String[] myAddressInfo = tokenizedLines.get(1 + nodeId);
        InetSocketAddress socketAddress = new InetSocketAddress(myAddressInfo[1], Integer.parseInt(myAddressInfo[2]));

        // 4. Create the node object
        int totalNodes = Integer.parseInt(tokenizedLines.get(0)[0]);
        Node node = new Node(totalNodes, Integer.parseInt(tokenizedLines.get(0)[3]), exponentialDelay, socketAddress);

        Set<String> quorumMemberIds = new HashSet<String>(Arrays.asList(tokenizedLines.get(1 + totalNodes + nodeId)));

        for( String[] nodeAddress : tokenizedLines.subList(1, 1 + totalNodes) ) {

            Integer id = new Integer(nodeAddress[0]);
            InetSocketAddress qualifiedAddress = new InetSocketAddress(nodeAddress[1], Integer.parseInt(nodeAddress[2]));

            Node.nodeAddresses.put(id, qualifiedAddress);

            if( quorumMemberIds.contains(nodeAddress[0]) ) {
                Node.quorumMembers.put(id, qualifiedAddress);
            }

        }

        return node;

    }

}
