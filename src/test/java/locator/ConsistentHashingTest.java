package locator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import shard.Node;
import utility.hash.Crc16; // Assuming Crc16 is in this package
import utility.HashFunction; // Assuming HashFunction is in this package
import locator.rebalancer.ReBalancerConsistHashing; // Full path to ReBalancerConsistHashing

import java.util.Arrays;
import java.util.List;

class ConsistentHashingTest {

    // Inner class for Mock ReBalancer
    static class MockReBalancerConsistHashing extends ReBalancerConsistHashing {
        public int movedDataCount = 0;
        public Node lastSrcNode = null;
        public Node lastDstNode = null;
        public int lastLowerBound = -1;
        public int lastUpperBound = -1;

        public MockReBalancerConsistHashing(HashFunction hashFunction, DataPlacement dataPlacement) {
            super(hashFunction, dataPlacement);
        }

        @Override
        public int moveData(Node srcNode, Node dstNode, int lowerBound, int upperBound) {
            this.lastSrcNode = srcNode;
            this.lastDstNode = dstNode;
            this.lastLowerBound = lowerBound;
            this.lastUpperBound = upperBound;

            int simulatedMoves = 0;
            // Simulate some data movement or just record call
            // This mock won't actually move data from srcNode.getEntries() in this simple version
            // For more advanced tests, you might simulate moving some dummy entries based on bounds.
            // For example, if srcNode had entries, you could iterate and "move" a few.
            this.movedDataCount += simulatedMoves;
            return simulatedMoves;
        }
    }

    // Inner class for Testable ConsistentHashing
    static class TestableConsistentHashing extends ConsistentHashing {
        private MockReBalancerConsistHashing mockReBalancerInstance;

        public TestableConsistentHashing(Config config) {
            super(config);
        }

        @Override
        protected ReBalancerConsistHashing createReBalancer() {
            // this.hashFunction is accessible via getHashFunction() from the super class
            this.mockReBalancerInstance = new MockReBalancerConsistHashing(this.getHashFunction(), this);
            return this.mockReBalancerInstance;
        }

        public MockReBalancerConsistHashing getMockReBalancer() {
            return this.mockReBalancerInstance;
        }
    }

    private TestableConsistentHashing testableConsistentHashing;
    private Config basicConfig;

    @BeforeEach
    void setUp() {
        List<String> initialNodes = Arrays.asList("node0", "node1");
        basicConfig = new Config.Builder()
                            .setHashFunction(new Crc16())
                            .setInitNodes(initialNodes)
                            .setNumOfVNode(3) // Default from Driver, can be adjusted
                            .setReplicaFactor(3) // Default from Driver
                            .build();
        testableConsistentHashing = new TestableConsistentHashing(basicConfig);
    }

    @Test
    void testAddAndLocateNode() {
        // Use a standard ConsistentHashing instance for this test as it doesn't rely on the mock rebalancer
        ConsistentHashing ch = new ConsistentHashing(basicConfig);

        Node addedNode = ch.addNode("testNode2");
        assertNotNull(addedNode, "Added node should not be null");
        assertTrue(ch.getNodes().stream().anyMatch(n -> n.getBucketID().equals("testNode2")),
                   "Node 'testNode2' should exist in the cluster after adding.");

        // This part of the test is more of an integration test for locateNode
        // as it depends on actual hash distribution.
        // A simple check:
        Node locatedNode = ch.locateNode("someKey123");
        assertNotNull(locatedNode, "Located node should not be null for any key if nodes exist.");

        // To make it more predictable, one might need to know tokens.
        // For example, find a key that maps to a specific node.
        // This is complex without more access to internal token states or a predictable hash.
    }

    @Test
    void testReBalancingToCallsMoveData() {
        // testableConsistentHashing is already initialized in setUp with "node0" and "node1"
        MockReBalancerConsistHashing mockReBalancer = testableConsistentHashing.getMockReBalancer();
        assertNotNull(mockReBalancer, "Mock rebalancer should have been set in TestableConsistentHashing");

        // Reset mock state before the action
        mockReBalancer.movedDataCount = 0;
        mockReBalancer.lastSrcNode = null;
        mockReBalancer.lastDstNode = null;
        mockReBalancer.lastLowerBound = -1;
        mockReBalancer.lastUpperBound = -1;

        Node newNode = testableConsistentHashing.addNode("nodeC"); // Add a new node

        // reBalancingTo is called internally by addNode if nodes already exist and then by explicit call.
        // The rebalancer is used during addNode to initialize the node (potentially)
        // and then reBalancingTo specifically calls it.
        // We are interested in the call from reBalancingTo(newNode) specifically.
        // The addNode itself might make the rebalancer do work if it's part of node initialization logic
        // that involves data movement (though typically addNode just places the node on the ring).
        // Let's reset the mock again right before the specific call we want to test,
        // assuming addNode itself doesn't call the specific reBalanceWithLowerUpperBound via reBalancer.moveData
        // in a way that would conflict with this test's assertions.
        // The current ConsistentHashing.addNode doesn't seem to trigger rebalancing, it just adds.
        // Rebalancing is a separate explicit step.

        mockReBalancer.movedDataCount = 0; // Reset again for clarity for the specific call
        mockReBalancer.lastSrcNode = null;
        mockReBalancer.lastDstNode = null;
        mockReBalancer.lastLowerBound = -1;
        mockReBalancer.lastUpperBound = -1;

        testableConsistentHashing.reBalancingTo(newNode);

        assertNotNull(mockReBalancer.lastDstNode, "reBalancingTo should have called moveData on rebalancer, dstNode should be set.");
        assertEquals(newNode.getBucketID(), mockReBalancer.lastDstNode.getBucketID(), "Destination node in moveData should be the new node.");
        assertNotNull(mockReBalancer.lastSrcNode, "Source node for rebalancing should have been identified by reBalancingTo.");

        // Further checks for lastLowerBound and lastUpperBound could be added if their expected values are known.
        // For example, lastUpperBound should be newNode.getToken().
        assertEquals(newNode.getToken(), mockReBalancer.lastUpperBound, "Upper bound in moveData should be the new node's token.");
    }
}
