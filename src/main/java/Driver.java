import locator.*;
import maptable.jHashMap;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.*;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shard.Node;
import utility.hash.Crc16;

import java.util.*;

public class Driver {

    private static final Level logLevel = Level.INFO;

    static {
        ConfigurationBuilder<BuiltConfiguration> builder
                = ConfigurationBuilderFactory.newConfigurationBuilder();
        // console
        AppenderComponentBuilder console
                = builder.newAppender("stdout", "Console");
        LayoutComponentBuilder standard
                = builder.newLayout("PatternLayout");
        standard.addAttribute("pattern", "%d [%t] %-5level: %msg%n%throwable");
        console.add(standard);
        builder.add(console);
        // root logger
        RootLoggerComponentBuilder rootLogger
                = builder.newRootLogger(logLevel);
        rootLogger.add(builder.newAppenderRef("stdout"));

        builder.add(rootLogger);
        Configurator.initialize(builder.build());
    }

    private static class HashingStrategiesDemo {

        private static final Logger logger = LoggerFactory.getLogger(HashingStrategiesDemo.class);

        // TODO: This method is for verification and ideally should be part of a dedicated test class.
        private void verifyData(Map<Integer,Integer> validate,
                                    jHashMap<Integer,Integer> candidate1,
                                    jHashMap<Integer,Integer> candidate2) {

            for (Map.Entry<Integer, Integer> kv : validate.entrySet()) {
                Integer val = candidate1.get(kv.getKey());
                if(val==null) {
                    throw new IllegalStateException(
                            String.format("Wrong value on candidate1 for %d(%04x)",
                                    kv.getKey(), kv.getKey()));
                }
                if (val.intValue() != kv.getValue().intValue()) {
                    throw new IllegalStateException("Wrong value");
                }
                val = candidate2.get(kv.getKey());
                if(val==null) {
                    throw new IllegalStateException(
                            String.format("Wrong value on candidate2 for %d(%04x)",
                                    kv.getKey(), kv.getKey()));
                }
                if (val.intValue() != kv.getValue().intValue()) {
                    throw new IllegalStateException("Wrong value");
                }
            }
        }

        public void start() {

            int numOfNodes = 5;
            List<String> nodeAddresses = new ArrayList<>(numOfNodes);
            for(int i=0; i<numOfNodes; i++) {
                nodeAddresses.add("Undefined_" + i);
            }
            Config config = new Config.Builder().setHashFunction(new Crc16())
                                                .setInitNodes(nodeAddresses)
                                                .setNumOfVNode(3)
                                                .setReplicaFactor(3)
                                                .build();

            Map<Integer, Integer> javaMap = new HashMap<>();
            DataPlacement modular = new Modular(config);
            DataPlacement consistentRing = new ConsistentHashing(config);
            DataPlacement withVirtualNode = new ConsistentHashingVNode(config);
            jHashMap<Integer, Integer> myMapChaining = new jHashMap<>(modular);
            jHashMap<Integer, Integer> myMapConsistent = new jHashMap<>(consistentRing);
            jHashMap<Integer, Integer> myMapConsistentVD = new jHashMap<>(withVirtualNode);

            // put data
            Random rand = new Random();
            for (int k = 0; k < 1000; k++) {
                int v = rand.nextInt(1000);
                myMapChaining.put(k, v);
                myMapConsistent.put(k, v);
                myMapConsistentVD.put(k,v);
                javaMap.put(k, v);
            }

            logger.info(String.format("Init. Total number of entries is %d%n", javaMap.size()));
            logger.info(myMapChaining.toString());
            logger.info(myMapConsistent.toString());
            logger.info(myMapConsistentVD.toString());
            logger.info(String.format("SD for %s is %.2f",
                    myMapChaining.getPlacementLogic(), myMapChaining.getStandardDeviation()));
            logger.info(String.format("SD for %s is %.2f",
                    myMapConsistent.getPlacementLogic(), myMapConsistent.getStandardDeviation()));
            logger.info(String.format("SD for %s is %.2f",
                    myMapConsistentVD.getPlacementLogic(), myMapConsistentVD.getStandardDeviation()));
            verifyData(javaMap, myMapChaining, myMapConsistent);
            verifyData(javaMap, myMapConsistent, myMapConsistentVD);

            // not easy to handle if two nodes are assigned on the ring and then migrate happens one by one.
            // failure case. new node are assigned at eb, f3. previous nodes c7 and f4.
            // migrate f3 from f4 -> lower bound is eb for f3. So less of eb value will not be moved
            // migrate eb from f3
            // todo: lower bound might not be required...but for vnode...
            //       need to think of that multiple nodes joined and then trigger rebalancing
            Node newNode1 = consistentRing.addNode("test1");
            int moveCnt = consistentRing.reBalancingTo(newNode1);
            logger.info("Moved " + moveCnt + " to " + newNode1.toString());
            verifyData(javaMap, myMapConsistent, myMapConsistentVD);

            moveCnt = consistentRing.reBalancingFrom(newNode1);
            logger.info("Moved " + moveCnt + " from " + newNode1.toString());
            consistentRing.rmNode(newNode1);
            verifyData(javaMap, myMapConsistent, myMapConsistentVD);

            newNode1 = withVirtualNode.addNode("test1");
            moveCnt = withVirtualNode.reBalancingTo(newNode1);
            logger.info("Moved " + moveCnt + " to " + newNode1.toString());
            logger.info(myMapConsistentVD.toString());
            verifyData(javaMap, myMapConsistent, myMapConsistentVD);

            //modular.addNode("test1");

            logger.info("Completed");
        }

    }

    public static void main (String[] args) throws Exception {
        HashingStrategiesDemo hashMapDriver = new HashingStrategiesDemo();
        hashMapDriver.start();
    }

}
