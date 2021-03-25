package locator;

import shard.Node;

import java.util.Collection;

public interface DataPlacement {

    Node addNode(String nodeAddress);
    void rmNode(Node node);

    int reBalancingTo(Node newNode);
    int reBalancingFrom(Node oldNode);

    Node locateNode(Object key);
    Collection<Node> getNodes();

    String getName();
}
