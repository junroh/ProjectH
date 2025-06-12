package locator.rebalancer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import shard.Node;
import shard.Entry;
import utility.HashFunction;
import locator.DataPlacement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ReBalancerConsistHashingTest {

    // --- Mock/Stub Inner Classes ---

    static class MockNode implements Node {
        private String id;
        private int token;
        private List<Entry> entries = new ArrayList<>();

        public MockNode(String id, int token) {
            this.id = id;
            this.token = token;
        }

        @Override public String getBucketID() { return id; }
        @Override public int getToken() { return token; }
        @Override public void setToken(int token) { this.token = token; }
        @Override public List<Entry> getEntries() { return entries; }
        @Override public void put(Entry entry) { entries.add(entry); }
        @Override public Entry get(Object key) {
            return entries.stream().filter(e -> e.getKey().equals(key)).findFirst().orElse(null);
        }
        @Override public Entry remove(Object key) {
            Entry entry = get(key);
            if (entry != null) {
                entries.remove(entry);
            }
            return entry;
        }
        @Override public void setBucketID(String id) { this.id = id; }
        @Override public int getNumOfEntries() { return entries.size(); }
    }

    static class MockDataPlacement implements DataPlacement {
        private Map<Object, Node> keyNodeMap = new HashMap<>();

        public void mapKeyToNode(Object key, Node node) {
            keyNodeMap.put(key, node);
        }

        @Override public Node locateNode(Object key) { return keyNodeMap.get(key); }
        @Override public Node addNode(String nodeAddress) { return null; } // Not used in these tests
        @Override public void rmNode(Node node) {} // Not used
        @Override public int reBalancingTo(Node newNode) { return 0; } // Not used
        @Override public int reBalancingFrom(Node oldNode) { return 0; } // Not used
        @Override public Collection<Node> getNodes() { return new ArrayList<>(keyNodeMap.values()); }
        @Override public String getName() { return "mockPlacement"; }
    }

    static class MockHashFunction implements HashFunction {
        @Override public int hf(Object key) {
            // Using simple hashCode for testing.
            // In ReBalancer, the hash function itself isn't directly used in moveData,
            // but rather the result of dataPlacement.locateNode(key).getToken().
            // So, this mock is mainly for completeness of ReBalancerConsistHashing constructor.
            return key.hashCode();
        }
    }

    static class SimpleEntry implements Entry {
        private Object key;
        private Object value;

        public SimpleEntry(Object key, Object value) {
            this.key = key;
            this.value = value;
        }
        @Override public Object getKey() { return key; }
        @Override public Object getValue() { return value; }
        @Override public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            SimpleEntry that = (SimpleEntry) obj;
            return key.equals(that.key) && value.equals(that.value);
        }
        @Override public int hashCode() {
            return java.util.Objects.hash(key, value);
        }
    }

    // --- Test Class Fields ---
    private MockHashFunction mockHashFn;
    private MockDataPlacement mockPlacement;
    private ReBalancerConsistHashing reBalancer;
    private MockNode srcNode;
    private MockNode dstNode;

    @BeforeEach
    void setUp() {
        mockHashFn = new MockHashFunction();
        mockPlacement = new MockDataPlacement();
        reBalancer = new ReBalancerConsistHashing(mockHashFn, mockPlacement);

        // Basic setup, can be overridden in tests
        srcNode = new MockNode("srcNode", 2000);
        dstNode = new MockNode("dstNode", 1000);
    }

    @Test
    void testMoveData_BasicScenario() {
        // dstNode's token is 1000.
        // lowerBound is 500.
        // Range for dstNode: (500, 1000] (exclusive lower, inclusive upper)

        Entry entryToMove = new SimpleEntry("key1", "value1"); // Should move
        Entry entryToKeep = new SimpleEntry("key2", "value2"); // Should keep

        srcNode.put(entryToMove);
        srcNode.put(entryToKeep);

        // IMPORTANT: For ReBalancer.moveData, the critical part is what dataPlacement.locateNode(key) returns.
        // The token of THAT node is checked against lowerBound and upperBound.
        // So, we need to make locateNode(entryToMove.getKey()) return a node whose token falls into the range.
        // Let's simulate that locateNode(entryToMove.getKey()) returns a conceptual node that IS dstNode.
        mockPlacement.mapKeyToNode(entryToMove.getKey(), dstNode); // This entry's key maps to dstNode (token 1000)

        // entryToKeep.getKey() should map to a node OUTSIDE the (lowerBound, upperBound] range for dstNode.
        // For simplicity, map it to srcNode itself (token 2000), which is outside (500, 1000].
        mockPlacement.mapKeyToNode(entryToKeep.getKey(), srcNode);

        int lowerBound = 500;
        int upperBound = dstNode.getToken(); // 1000

        int movedCount = reBalancer.moveData(srcNode, dstNode, lowerBound, upperBound);

        assertEquals(1, movedCount, "Should have moved 1 entry.");
        assertEquals(1, dstNode.getEntries().size(), "Destination node should have 1 entry.");
        assertTrue(dstNode.getEntries().contains(entryToMove), "Destination node should contain the moved entry.");
        assertEquals(1, srcNode.getEntries().size(), "Source node should have 1 entry remaining.");
        assertTrue(srcNode.getEntries().contains(entryToKeep), "Source node should still contain the entry it keeps.");
    }

    @Test
    void testMoveData_ReverseCase() {
        // Scenario: Ring wraps around. Example: dstNode covers tokens (800, MAX_RING_VALUE] U [0, 200].
        // In ReBalancer.moveData, if lowerBound > upperBound, it's a reverse case.
        // The condition for moving an entry becomes:
        //   tokenOfLocatedNode > lowerBound  OR  tokenOfLocatedNode <= upperBound

        // dstNode is where data moves TO.
        // For the reverseCase logic, dstNode.getToken() is treated as the `upperBound` for the second part of the range.
        dstNode.setToken(200); // Data for nodes with token <= 200 should move.
        int lowerBound = 800;  // Data for nodes with token > 800 should also move.
                               // This `lowerBound` is typically the token of the node that logically precedes `dstNode` on the ring.

        // srcNode is where data moves FROM. Its token doesn't directly play into the range logic here,
        // only the tokens of the nodes returned by mockPlacement.locateNode(key).
        srcNode.setToken(500); // Arbitrary token for srcNode, not in the move range.


        Entry entryToMoveWrap = new SimpleEntry("keyWrap", "valueWrap");     // e.g., locateNode returns a node with token 150 (<= 200)
        Entry entryToMoveDirect = new SimpleEntry("keyDirect", "valueDirect"); // e.g., locateNode returns a node with token 900 (> 800)
        Entry entryToKeep = new SimpleEntry("keyKeep", "valueKeep");         // e.g., locateNode returns a node with token 600 (not in range)

        srcNode.put(entryToMoveWrap);
        srcNode.put(entryToMoveDirect);
        srcNode.put(entryToKeep);

        // Configure MockDataPlacement:
        // Create temporary mock nodes just to return specific tokens from locateNode, matching dstNode's responsibility.
        MockNode nodeForWrapKey = new MockNode("nodeForKeyWrap", 150);
        MockNode nodeForDirectKey = new MockNode("nodeForKeyDirect", 900);
        MockNode nodeForKeyKeep = new MockNode("nodeForKeyKeep", 600);

        mockPlacement.mapKeyToNode(entryToMoveWrap.getKey(), nodeForWrapKey);    // token 150 <= 200 (upperBound for dstNode)
        mockPlacement.mapKeyToNode(entryToMoveDirect.getKey(), nodeForDirectKey); // token 900 > 800 (lowerBound)
        mockPlacement.mapKeyToNode(entryToKeep.getKey(), nodeForKeyKeep);       // token 600 is not in range

        int movedCount = reBalancer.moveData(srcNode, dstNode, lowerBound, dstNode.getToken()); // upperBound is 200

        assertEquals(2, movedCount, "Should have moved 2 entries in reverse case.");
        assertEquals(2, dstNode.getEntries().size(), "Destination node should have 2 entries.");
        assertTrue(dstNode.getEntries().contains(entryToMoveWrap), "Dst should contain keyWrap.");
        assertTrue(dstNode.getEntries().contains(entryToMoveDirect), "Dst should contain keyDirect.");
        assertEquals(1, srcNode.getEntries().size(), "Source node should have 1 entry remaining.");
        assertTrue(srcNode.getEntries().contains(entryToKeep), "Src should still contain keyKeep.");
    }
}
