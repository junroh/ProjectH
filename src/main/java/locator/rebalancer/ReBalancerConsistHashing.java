package locator.rebalancer;

import locator.DataPlacement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shard.Node;
import shard.Entry;
import utility.HashFunction;

import java.util.Iterator;

public class ReBalancerConsistHashing {

    private static final Logger logger = LoggerFactory.getLogger(ReBalancerConsistHashing.class);
    private final HashFunction hashFunction;
    private final DataPlacement dataPlacement;

    public ReBalancerConsistHashing(HashFunction hashFunction, DataPlacement dataPlacement) {
        this.hashFunction = hashFunction;
        this.dataPlacement = dataPlacement;
    }

    // handling a migration on same physical node should be covered before calling this one
    public int moveData(Node srcNode, Node dstNode, int lowerBound, int upperBound) {
        int numOfMoves = 0;
        if(srcNode == null || dstNode == null) {
            return numOfMoves;
        }
        boolean reverseCase = false;
        if(lowerBound>upperBound) {
            // upperbound node is placed less than 0
            reverseCase = true;
        }
        // TODO: This can be used sorted list
        try {
            Iterator<Entry> itr = srcNode.getEntries().iterator();
            while (itr.hasNext()) {
                Entry entry = itr.next();
                int token = dataPlacement.locateNode(entry.getKey()).getToken();
                boolean requireMove = false;
                if(!reverseCase) {
                    if (token > lowerBound && token <= upperBound) {
                        requireMove = true;
                    }
                } else {
                    if (token > lowerBound || token <= upperBound) {
                        requireMove = true;
                    }
                }
                if(requireMove) {
                    dstNode.put(entry);
                    itr.remove();
                    numOfMoves++;
                }
            }
            logger.debug(
                    String.format("Migrate keys from token %04x to token %04x(from %04x) completed. Total %d",
                    srcNode.getToken(), upperBound, lowerBound, numOfMoves));
        } catch (Exception e) {
            logger.error(
                    String.format("Failed to migrate keys from token %04x to token %04x(from %04x) due to %s",
                    srcNode.getToken(), upperBound, lowerBound, e));
        }
        return numOfMoves;
    }

}
