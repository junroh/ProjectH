package maptable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tools4j.meanvar.MeanVarianceSampler;
import shard.Node;
import shard.Entry;
import locator.DataPlacement;
import shard.PhysicalNode;

import java.util.*;

public class jHashMap<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(jHashMap.class);
    private final DataPlacement dataPlacement;
    private int count;

    public jHashMap(DataPlacement dataPlacement) {
        this.dataPlacement = dataPlacement;
    }

    public V put(K k, V v) {
        V oldV = null;
        Node node = dataPlacement.locateNode(k);
        Entry kv = node.get(k);
        if (kv == null) {
            count++;
            node.put(new Entry(k, v));
        } else {
            // allow update
            oldV = (V)kv.getValue();
            kv.setValue(v);
        }
        return oldV;
    }

    public V get(K k) {
        Node node = dataPlacement.locateNode(k);
        Entry kv = node.get(k);
        return (kv != null) ? (V)kv.getValue() : null;
    }

    public boolean remove(K k) {
        boolean ret = false;
        Node node = dataPlacement.locateNode(k);
        Entry kv = node.get(k);
        if(kv!=null) {
            ret = node.remove(kv);
        }
        return ret;
    }

    public int size() {
        return count;
    }

    public List<K> getKeys() {
        List<K> keys = new ArrayList<>(size());
        for(Node bucket: dataPlacement.getNodes()) {
            for(Entry node: bucket.getEntries()) {
                keys.add((K)node.getKey());
            }
        }
        return keys;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder(dataPlacement.getName());
        stringBuilder.append(String.format(" SD: %.2f", getStandardDeviation()));
        Set<String> checkedNode = new HashSet<>();
        for(Node node : dataPlacement.getNodes()) {
            if(!logger.isDebugEnabled()) {
                Node pNode = node.getUnderlyingNode();
                if (pNode != null) {
                    if (!checkedNode.add(pNode.getBucketID())) {
                        continue;
                    }
                    node = pNode;
                }
            }
            float percent = node.getEntries().size()/(float)size()*100;
            stringBuilder.append(String.format("%n(%6.2f%%) ", percent));
            stringBuilder.append(node.toString());
        }
        return stringBuilder.toString();
    }

    public String getPlacementLogic() {
        return dataPlacement.getName();
    }

    public double getStandardDeviation() {
        Set<Node> physicalNodes = new HashSet<>();
        for(Node node : dataPlacement.getNodes()) {
            Node pNode = node.getUnderlyingNode();
            if(pNode!=null) {
                node = pNode;
            }
            physicalNodes.add(node);
        }
        double mean = (double)size()/ physicalNodes.size();
        double standardDeviation = 0.0;
        for(Node node : physicalNodes) {
            standardDeviation += Math.pow(node.getEntries().size() - mean, 2);
        }
        return Math.sqrt(standardDeviation/size());
    }

    // in-place update
    public void reHash(int size) {
        /*
        List<Bucket> copy = buckets;
        initialiseMap(size);
        for (Bucket bucket: copy) {
            for(Entry entry: bucket.getEntries()) {
                put(entry.getKey(), entry.getValue());
            }
        }
        
         */
    }
}
