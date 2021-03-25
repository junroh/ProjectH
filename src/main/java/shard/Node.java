package shard;

import java.util.List;

public interface Node {

    List<Entry> getEntries();

    void put(Entry entry);

    Entry get(Object k);

    boolean remove(Entry entry);

    int getToken();

    void setToken(int pos);

    String getBucketID();

    void registerVNode(Node node);

    List<Node> getVirtualNodes();

    Node getUnderlyingNode();
}
