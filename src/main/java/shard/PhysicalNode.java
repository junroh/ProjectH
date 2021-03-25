package shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class PhysicalNode implements Node {

    private static final Logger logger = LoggerFactory.getLogger(PhysicalNode.class);
    private final List<Entry> entries;
    private final String nodeAddress;
    private final String bucketID;
    private final List<Node> virtual;
    private int token;

    public PhysicalNode(String nodeAddress) {
        this.token = -1;
        this.nodeAddress = nodeAddress;
        this.entries = new LinkedList<>();
        this.virtual = new LinkedList<>();
        this.bucketID = UUID.randomUUID().toString();
    }

    public List<Entry> getEntries() {
        return entries;
    }

    public void put(Entry entry) {
        entries.add(entry);
    }

    public Entry get(Object k) {
        for (Entry entry : entries) {
            if (k.equals(entry.getKey())) {
                return entry;
            }
        }
        return null;
    }

    public boolean remove(Entry entry) {
        return entries.remove(entry);
    }

    public int getToken() {
        return token;
    }

    public void setToken(int token) {
        this.token = token;
    }

    public String getBucketID() {
        return bucketID;
    }

    public void registerVNode(Node node) {
        virtual.add(node);
    }

    public List<Node> getVirtualNodes() {
        return virtual;
    }

    @Override
    public Node getUnderlyingNode() {
        return null;
    }

    @Override
    public String toString() {
        String str;
        if(logger.isDebugEnabled()) {
            str = String.format("pNode(%s(%s) @%04x(%d)) - %s",
                    bucketID, nodeAddress, token, entries.size(), entries.toString());
        } else {
            str = String.format("pNode(%s(%s) @%04x(%d))",
                    bucketID, nodeAddress, token, entries.size());
        }
        return str;
    }
}
