package locator;

import shard.Node;
import shard.PhysicalNode;
import utility.HashFunction;

import java.util.*;

public class Modular implements DataPlacement {

    private final HashFunction hashFunction;
    private List<Node> nodes;
    private int numofBuckets;

    public Modular(Config config) {
        hashFunction = config.getHashFunction();
        initialiseMap(config.getNodes());
    }

    @Override
    public String getName() {
        return "modular";
    }

    @Override
    public Node addNode(String nodeAddress) {
        Node node = new PhysicalNode(nodeAddress);
        node.setToken(numofBuckets);
        addNode(node);
        return node;
    }

    public void addNode(Node node) {
        int token = node.getToken();
        if(token>numofBuckets) {
            throw new IndexOutOfBoundsException();
        }
        for(int i=token;i<numofBuckets;i++) {
            nodes.get(i).setToken(i+1);
        }
        nodes.add(token, node);
        numofBuckets++;
    }

    @Override
    public void rmNode(Node node) {
        if(numofBuckets==1) {
            throw new IndexOutOfBoundsException("Number of bucket reaches to 1");
        }
        int token = node.getToken();
        if(token>=numofBuckets) {
            throw new IndexOutOfBoundsException();
        }
        Node removed = nodes.get(token);
        if(removed==null) {
            throw new IllegalArgumentException("Couldn't find corresponding bucket");
        }
        nodes.remove(removed);
        numofBuckets--;
        for(int i=token;i<numofBuckets;i++) {
            nodes.get(i).setToken(i);
        }
    }

    @Override
    public Node locateNode(Object key) {
        return nodes.get(getBucketToken(key));
    }

    @Override
    public Collection<Node> getNodes(){
        return nodes;
    }

    private int getBucketToken(Object key) {
        int hash = hashFunction.hf(key);
        return hash%numofBuckets;
    }

    @Override
    public int reBalancingTo(Node newNode) {
        return 0;
    }

    @Override
    public int reBalancingFrom(Node oldNode) {
        return 0;
    }

    private void initialiseMap(List<String> nodeIDs) {
        nodes = new LinkedList<>();
        for(String nodeID: nodeIDs) {
            addNode(nodeID);
        }
        numofBuckets = nodes.size();
    }

}
