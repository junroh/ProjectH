package locator;

import utility.HashFunction;
import utility.hash.Crc16;

import java.util.List;

public class Config {

    private final HashFunction hashFunction;
    private int numOfVNode;
    private int replicaFactor;
    private List<String> nodes;

    private Config(HashFunction hashFunction) {
        this.hashFunction = hashFunction;
    }

    public int getNumOfvNode() {
        return numOfVNode;
    }

    public HashFunction getHashFunction() {
        return hashFunction;
    }

    public List<String> getNodes() {
        return nodes;
    }

    public int getReplicaFactor() {
        return replicaFactor;
    }

    private void setNumOfVNode(int n) {
        this.numOfVNode = n;
    }

    private void setInitNodes(List<String> nodes) {
        this.nodes = nodes;
    }

    private void setReplicaFactor(int rf) {
        this.replicaFactor = rf;
    }


    public static class Builder {
        private HashFunction hashFunction = new Crc16();
        private int numOfVnode = 3;
        private int replicaFactor = 1;
        private List<String> nodes;

        public Builder setHashFunction(HashFunction hashFunction) {
            this.hashFunction = hashFunction;
            return this;
        }

        public Builder setNumOfVNode(int vnode) {
            this.numOfVnode = vnode;
            return this;
        }

        public Builder setInitNodes(List<String> nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder setReplicaFactor(int rf) {
            this.replicaFactor = rf;
            return this;
        }

        public Config build() {
            Config cfg = new Config(hashFunction);
            cfg.setNumOfVNode(numOfVnode);
            cfg.setInitNodes(nodes);
            cfg.setReplicaFactor(replicaFactor);
            return cfg;
        }
    }
}
