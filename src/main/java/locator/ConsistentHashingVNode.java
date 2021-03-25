package locator;

import shard.Node;
import shard.PhysicalNode;
import shard.VirtualNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ConsistentHashingVNode extends ConsistentHashing {

    private static final Logger logger = LoggerFactory.getLogger(ConsistentHashingVNode.class);
    private int numOfVirtualNode;

    public ConsistentHashingVNode(Config config) {
        super(config);
    }

    @Override
    public String getName() {
        return "consistent hashing with vNode(" + numOfVirtualNode +")";
    }

    @Override
    public Node addNode(String nodeAddress) {
        List<Node> physicalNodes = new LinkedList<>();
        physicalNodes.add(new PhysicalNode(nodeAddress));
        placeNodeRandomly(physicalNodes);
        return physicalNodes.get(0);
    }

    @Override
    public int reBalancingTo(Node newNode) {
        if(newNode.getUnderlyingNode()!=null) {
            throw new IllegalArgumentException("Given node should be physical node");
        }
        List<Node> vNodes = new ArrayList<>(newNode.getVirtualNodes());
        vNodes.sort(Comparator.comparingInt(Node::getToken));

        int moveCnt = 0;
        List<Node> mergedNodes = new LinkedList<>();
        String physicalNodeID = newNode.getBucketID();
        for (Node vDstNode : vNodes) {
            if(mergedNodes.contains(vDstNode)) {
                continue;
            }
            Node srcNode = getNodeByToken(vDstNode.getToken() + 1);
            if (srcNode == null) {
                // This happens when only 1 node is on the ring
                continue;
            } else if (physicalNodeID.equals(srcNode.getUnderlyingNode().getBucketID())) {
                logger.debug(String.format(
                        "No need to migrate @%04x to @%04x because they are on the same physical node",
                        srcNode.getToken(), vDstNode.getToken()));
                continue;
            }
            int lowerBound;
            try {
                lowerBound = getLowerBound(mergedNodes, vDstNode, physicalNodeID);
            } catch (Exception e) {
                continue;
            }
            moveCnt += reBalanceWithLowerUpperBound(srcNode, vDstNode, lowerBound);
        }
        return moveCnt;
    }

    private int getLowerBound(List<Node> mergedNodes, Node vDstNode, String physicalNodeID) {
        Node prevOfDst = getPrevNodeByToken(vDstNode.getToken());
        if(prevOfDst==null) {
            // This happens when only 1 node is on the ring
            throw new IllegalStateException("Couldn't find previous Node");
        }
        int lowerBound = prevOfDst.getToken();
        while (true) {
            if (prevOfDst == null) {
                break;
            } else if(!physicalNodeID.equals(prevOfDst.getUnderlyingNode().getBucketID())) {
                lowerBound = prevOfDst.getToken();
                break;
            }
            mergedNodes.add(prevOfDst);
            int skipToken = prevOfDst.getToken();
            prevOfDst = getPrevNodeByToken(skipToken);
            int prevToken = -1;
            if(prevOfDst!=null) {
                prevToken = prevOfDst.getToken();
            }
            logger.debug(String.format("vNode(%04x<-%04x) was merged into new vNode@%04x",
                    skipToken, prevToken, vDstNode.getToken()));
        }
        return lowerBound;
    }

    private void placeNodeRandomly(List<Node> physicalNodes) {
        for(Node physicalNode: physicalNodes) {
            int success = 0;
            while(success<numOfVirtualNode) {
                Node newNode = new VirtualNode(physicalNode, success+1);
                newNode.setToken(getBucketToken(newNode.getBucketID()));
                if(addNode(newNode)) {
                    newNode.getUnderlyingNode().registerVNode(newNode);
                    success++;
                }
            }
        }
    }

    @Override
    protected void initialiseMap(Config config) {
        super.initialise();

        List<String> nodeAddresses = config.getNodes();
        numOfVirtualNode = config.getNumOfvNode();

        List<Node> physicalNodes = new LinkedList<>();
        for (String nodeAddress: nodeAddresses) {
            physicalNodes.add(new PhysicalNode(nodeAddress));
        }
        placeNodeRandomly(physicalNodes);
    }
}
