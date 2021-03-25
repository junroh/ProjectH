package shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class VirtualNode implements Node {

    private static final Logger logger = LoggerFactory.getLogger(VirtualNode.class);
    private final String bucketID;
    private final Node underlyingNode;
    private final List<Node> virtual;
    private int pos;

    public VirtualNode(int pos, Node underlyingNode, int idx) {
        this.pos = pos;
        this.bucketID = UUID.randomUUID().toString();
        this.underlyingNode = underlyingNode;
        this.virtual = new LinkedList<>();
    }

    public VirtualNode(Node underlyingNode, int idx) {
        this(-1, underlyingNode, idx);
    }

    public List<Entry> getEntries() {
        return underlyingNode.getEntries();
    }

    public void put(Entry entry) {
        underlyingNode.put(entry);
    }

    public Entry get(Object k) {
        return underlyingNode.get(k);
    }

    public boolean remove(Entry entry) {
        return underlyingNode.remove(entry);
    }

    public int getToken() {
        return pos;
    }

    public void setToken(int pos) {
        this.pos = pos;
    }

    public void registerVNode(Node node) {
        virtual.add(node);
    }

    @Override
    public Node getUnderlyingNode() {
        return underlyingNode;
    }

    @Override
    public List<Node> getVirtualNodes() {
        return virtual;
    }

    @Override
    public String getBucketID() {
        return bucketID;
    }

    @Override
    public String toString() {
        return String.format("vNode(%s @%04x) - %s",
                bucketID, pos, underlyingNode.toString());
    }
}
