package locator;

import locator.rebalancer.ReBalancerConsistHashing;
import shard.Node;
import shard.PhysicalNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.HashFunction;

import java.util.*;

public class ConsistentHashing implements DataPlacement {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashing.class);

    private static final int NUM_OF_SHARDS = Integer.MAX_VALUE;
    //private static final int NUM_OF_SHARDS = 1<<8;

    private final HashFunction hashFunction;
    private final int replicaFactor;

    protected ReBalancerConsistHashing reBalancer;
    protected SortedMap<Integer, Node> buckets;

    public ConsistentHashing(Config config) {
        this.hashFunction = config.getHashFunction();
        this.replicaFactor = config.getReplicaFactor();
        initialiseMap(config);
    }

    @Override
    public String getName() {
        return "consistent hashing";
    }

    @Override
    public Node addNode(String nodeAddress) {
        boolean isAdded;
        Node newNode;
        do {
            newNode = new PhysicalNode(nodeAddress);
            newNode.setToken(getBucketToken(newNode.getBucketID()));
            isAdded = addNode(newNode);
        } while(!isAdded);
        return newNode;
    }

    @Override
    public void rmNode(Node node) {
        if(buckets.isEmpty()) {
            throw new IndexOutOfBoundsException("Node nodes are on the list");
        }
        if(!node.getEntries().isEmpty()) {
            throw new IllegalStateException(node.getBucketID() + " is not empty");
        }
        if(buckets.remove(node.getToken())==null) {
            throw new IllegalArgumentException("Couldn't find corresponding bucket");
        }
    }

    @Override
    public Node locateNode(Object key) {
        int hash = hashFunction.hf(key);
        return getNodeByToken(hash);
    }

    @Override
    public Collection<Node> getNodes(){
        return buckets.values();
    }

    @Override
    public int reBalancingTo(Node dstNode) {
        Node srcNode = getNodeByToken(dstNode.getToken()+1);
        if(srcNode==null) {
            // This happens when only 1 node is on the ring
            return 0;
        }
        Node prevOfDst = getPrevNodeByToken(dstNode.getToken());
        if(prevOfDst==null) {
            // This happens when only 1 node is on the ring
            return 0;
        }
        int lowerBound = prevOfDst.getToken();
        return reBalanceWithLowerUpperBound(srcNode, dstNode, lowerBound);
    }

    @Override
    public int reBalancingFrom(Node srcNode) {
        Node dstNode = getNodeByToken(srcNode.getToken()+1);
        if(dstNode==null) {
            // This happens when only 1 node is on the ring
            return 0;
        }
        Node prevOfSrc = getPrevNodeByToken(srcNode.getToken());
        if(prevOfSrc==null) {
            // This happens when only 1 node is on the ring
            return 0;
        }
        int lowerBound = prevOfSrc.getToken();
        return reBalanceWithLowerUpperBound(srcNode, dstNode, lowerBound);
    }

    protected void initialise() {
        buckets = new TreeMap<>();
        reBalancer = new ReBalancerConsistHashing(hashFunction, this);
    }

    protected void initialiseMap(Config config) {
        initialise();
        List<String> nodeAddresses = config.getNodes();
        for(String nodeAddress: nodeAddresses) {
            try {
                addNode(nodeAddress);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to add bucket due to " + e);
            }
        }
    }

    protected int getBucketToken(Object key) {
        int hash = hashFunction.hf(key);
        return hash%NUM_OF_SHARDS;
    }

    protected boolean addNode(Node newNode) {
        int token = newNode.getToken();
        if(token>=NUM_OF_SHARDS) {
            throw new IndexOutOfBoundsException();
        }
        if(token<0) {
            throw new IllegalStateException("token is less than 0");
        }
        if(buckets.containsKey(token)) {
            return false;
        }
        logger.debug(String.format("Node was assigned at %04x",token));
        buckets.put(token, newNode);
        return true;
    }

    protected int reBalanceWithLowerUpperBound(Node srcNode, Node dstNode, int lowerBound) {
        logger.debug(String.format("Start migrate data from %04x(%d) to [%04x(%d)<-%04x)",
                srcNode.getToken(), srcNode.getEntries().size(),
                dstNode.getToken(), dstNode.getEntries().size(),
                lowerBound));
        return reBalancer.moveData(srcNode, dstNode, lowerBound, dstNode.getToken());
    }

    // return node which can have this token
    // toke is included
    protected Node getNodeByToken(int token) {
        token = token%NUM_OF_SHARDS;
        SortedMap<Integer, Node> subMap = buckets.tailMap(token);
        int closedToken;
        if(subMap.isEmpty()) {
            // Couldn't find larger. Return least one
            if(!buckets.isEmpty()) {
                closedToken = buckets.firstKey();
            } else {
                return null;
            }
        } else {
            //The first Key is the nearest bucket.
            closedToken = subMap.firstKey();
        }
        return buckets.get(closedToken);
    }

    // return node just before of this token
    // token is not included.
    protected Node getPrevNodeByToken(int token) {
        token = token%NUM_OF_SHARDS;
        SortedMap<Integer, Node> subMap = buckets.headMap(token);
        int closedToken;
        if(subMap.isEmpty()) {
            // Couldn't find smaller. Return the largest one
            if(!buckets.isEmpty()) {
                closedToken = buckets.lastKey();
                if(closedToken == token) {
                    return null;
                }
            } else {
                return null;
            }
        } else {
            closedToken = subMap.lastKey();
        }
        return buckets.get(closedToken);
    }
}
