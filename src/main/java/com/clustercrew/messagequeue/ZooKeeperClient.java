package com.clustercrew.messagequeue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ZooKeeperClient {
    private final ZooKeeper zk;
    private final PartitionAssigner partitionAssigner;

    public ZooKeeperClient(String zkServers) throws Exception {
        this.zk = new ZooKeeper(zkServers, 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("Connected to ZooKeeper");
                
            } else if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged
                    && event.getPath().equals("/brokers")) {
                try {
                    rebalanceAllTopics();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        this.partitionAssigner = new PartitionAssigner(this);
        ensurePathExists("/brokers");
        ensurePathExists("/topics");

    }

    /**
     * Ensures the given path exists in ZooKeeper.
     *
     * @param path The path to ensure.
     * @throws Exception If an error occurs while creating the path.
     */
    private void ensurePathExists(String path) throws Exception {
        if (zk.exists(path, false) == null) {
            String[] parts = path.split("/");
            StringBuilder currentPath = new StringBuilder();
            for (String part : parts) {
                if (part.isEmpty()) continue;
                currentPath.append("/").append(part);
                if (zk.exists(currentPath.toString(), false) == null) {
                    zk.create(currentPath.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
        }
    }

    // ---------------------------------- Topic Management ----------------------------------
    /**
     * Creates a new topic in ZooKeeper with the specified metadata.
     *
     * @param topic            The name of the topic.
     * @param numPartitions    The number of partitions.
     * @param retentionMs      The retention period in milliseconds.
     * @param replicationFactor The replication factor.
     * @throws Exception If an error occurs while creating the topic.
     */
    public void createTopic(String topic, int numPartitions, int retentionMs, int replicationFactor) throws Exception {
        String topicPath = "/topics/" + topic;
        ensurePathExists(topicPath);

        // Save metadata
        String metadata = String.format("partitions=%d,retention=%d,replicas=%d", numPartitions, retentionMs, replicationFactor);
        zk.setData(topicPath, metadata.getBytes(StandardCharsets.UTF_8), -1);

        // Create partitions
        for (int i = 0; i < numPartitions; i++) {
            ensurePathExists(topicPath + "/partitions/" + i);
        }
        System.out.println("Topic created: " + topic);

        // Assign partitions to available brokers
        List<String> activeBrokers = getActiveBrokers();
        partitionAssigner.assignPartitions(topic, numPartitions, activeBrokers);
    }

    /**
     * Retrieves the list of all topics stored in ZooKeeper.
     *
     * @return A list of topic names.
     * @throws Exception If an error occurs while fetching the topics.
     */
    public List<String> getTopics() throws Exception {
        return zk.getChildren("/topics", false);
    }

    /**
     * Checks if a topic exists in ZooKeeper.
     *
     * @param topic The topic name.
     * @return True if the topic exists, false otherwise.
     * @throws Exception If an error occurs during the check.
     */
    public boolean topicExists(String topic) throws Exception {
        String topicPath = "/topics/" + topic;
        return zk.exists(topicPath, false) != null;
    }

    /**
     * Rebalances all topics across the active brokers.
     */
    private void rebalanceAllTopics() throws Exception {
        List<String> activeBrokers = getActiveBrokers();
        List<String> topics = getTopics();

        if (activeBrokers.isEmpty()) {
            throw new Exception("No active brokers available for rebalancing.");
        }

        for (String topic : topics) {
            partitionAssigner.rebalancePartitions(topic, activeBrokers);
        }
    }

    // ---------------------------------- Partition Management ----------------------------------

    /**
     * Retrieves the list of partitions for a topic.
     *
     * @param topic The topic name.
     * @return A list of partition IDs.
     * @throws Exception If an error occurs while fetching partitions.
     */
    public List<String> getPartitions(String topic) throws Exception {
        String topicPath = "/topics/" + topic + "/partitions";
        ensurePathExists(topicPath);
        return zk.getChildren(topicPath, false);
    }

    /**
     * Checks if a partition exists for a topic in ZooKeeper.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return True if the partition exists, false otherwise.
     * @throws Exception If an error occurs during the check.
     */
    public boolean partitionExists(String topic, int partition) throws Exception {
        String partitionPath = "/topics/" + topic + "/partitions/" + partition;
        return zk.exists(partitionPath, false) != null;
    }

    // ---------------------------------- Broker Management ----------------------------------

    /**
     * Registers a broker in ZooKeeper.
     *
     * @param brokerId The ID of the broker.
     * @throws Exception If an error occurs while registering the broker.
     */
    public void registerBroker(String brokerId, String brokerAddress) throws Exception {
        String brokerPath = "/brokers/" + brokerId;

        // Create a persistent node for the broker to store partitions
        if (zk.exists(brokerPath, false) == null) {
            zk.create(brokerPath, brokerAddress.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            // Rebalance partitions across all topics
            rebalanceAllTopics();
        }
        
        System.out.println("Broker registered: " + brokerId + " with address: " + brokerAddress);
    }

    /**
     * Retrieves a list of active brokers from ZooKeeper.
     *
     * @return A list of broker IDs.
     * @throws Exception If an error occurs while fetching the broker list.
     */
    public List<String> getActiveBrokers() throws Exception {
        return zk.getChildren("/brokers", false);
    }

    /**
     * Retrieves the address of a broker by its ID.
     *
     * @param brokerId The broker ID.
     * @return The broker address.
     * @throws Exception If an error occurs while fetching the address.
     */
    public String getBrokerAddress(String brokerId) throws Exception {
        String brokerPath = "/brokers/" + brokerId;
        Stat stat = zk.exists(brokerPath, false);
        if (stat == null) {
            throw new Exception("Broker " + brokerId + " not found in ZooKeeper.");
        }
        byte[] data = zk.getData(brokerPath, false, null);
        return new String(data, StandardCharsets.UTF_8);
    }

    /**
     * Assigns a partition to a broker.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param brokerId  The broker ID.
     * @throws Exception If an error occurs while assigning the partition.
     */
    public void assignPartitionToBroker(String topic, int partition, String brokerId) throws Exception {
        String brokerPartitionPath = "/brokers/" + brokerId + "/topics/" + topic + "/partitions/" + partition;
        String topicPartitionPath = "/topics/" + topic + "/partitions/" + partition + "/broker";

        ensurePathExists(brokerPartitionPath);
        ensurePathExists(topicPartitionPath);

        // Store the partition number under the broker path
        zk.setData(brokerPartitionPath, String.valueOf(partition).getBytes(StandardCharsets.UTF_8), -1);
        // Store the broker ID under the topic partition path
        zk.setData(topicPartitionPath, brokerId.getBytes(StandardCharsets.UTF_8), -1);

        System.out.println("Partition " + partition + " of topic " + topic + " assigned to broker " + brokerId);
    }

    /**
     * Reassigns a partition to a new broker in the event of a failure.
     *
     * @param topic       The topic name.
     * @param partition   The partition number.
     * @param newBrokerId The new broker ID.
     * @throws Exception If an error occurs while reassigning the partition.
     */
    public void reassignPartition(String topic, int partition, String newBrokerId) throws Exception {
        String partitionPath = "/topics/" + topic + "/partitions/" + partition + "/broker";
        zk.setData(partitionPath, newBrokerId.getBytes(StandardCharsets.UTF_8), -1);
        System.out.println("Partition " + partition + " of topic " + topic + " reassigned to broker " + newBrokerId);
    }

    /**
     * Retrieves the broker responsible for a partition.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return The broker ID.
     * @throws Exception If an error occurs while fetching the broker.
     */
    public String getPartitionBroker(String topic, int partition) throws Exception {
        String path = "/topics/" + topic + "/partitions/" + partition + "/broker";
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            throw new Exception("No broker assigned for topic: " + topic + ", partition: " + partition);
        }
        byte[] data = zk.getData(path, false, null);
        return new String(data, StandardCharsets.UTF_8);
    }

    /**
     * Retrieves the list of partitions assigned to a broker.
     *
     * @param brokerId The ID of the broker.
     * @return List of partition IDs assigned to the broker.
     * @throws Exception If an error occurs while fetching the partitions.
     */
    public List<Integer> getAssignedPartitions(String brokerId) throws Exception {
        String brokerPath = "/brokers/" + brokerId + "/partitions";
        ensurePathExists(brokerPath);

        List<Integer> assignedPartitions = new ArrayList<>();
        List<String> partitionNodes = zk.getChildren(brokerPath, false);

        for (String partitionNode : partitionNodes) {
            assignedPartitions.add(Integer.parseInt(partitionNode));
        }

        return assignedPartitions;
    }

    // ---------------------------------- Ledger Management ----------------------------------

    /**
     * Adds a new ledger ID to the list of ledgers for a topic partition.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param ledgerId  The ledger ID to add.
     * @throws Exception If an error occurs while updating the ledger list.
     */
    public void addLedgerToPartition(String topic, int partition, long ledgerId) throws Exception {
        String path = "/topics/" + topic + "/partitions/" + partition + "/ledgers";
        ensurePathExists(path);

        // Fetch existing ledgers
        byte[] data = zk.getData(path, false, null);
        String currentLedgers = (data != null) ? new String(data, StandardCharsets.UTF_8) : "";
        List<String> ledgerList = new ArrayList<>(Arrays.asList(currentLedgers.split(",")));

        // Add new ledger and update ZooKeeper
        ledgerList.add(String.valueOf(ledgerId));
        zk.setData(path, String.join(",", ledgerList).getBytes(StandardCharsets.UTF_8), -1);
    }

    /**
     * Retrieves the list of ledger IDs for a topic partition.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return A list of ledger IDs.
     * @throws Exception If an error occurs while fetching the ledgers.
     */
    public List<Long> getPartitionLedgers(String topic, int partition) throws Exception {
        String path = "/topics/" + topic + "/partitions/" + partition + "/ledgers";
        ensurePathExists(path);

        byte[] data = zk.getData(path, false, null);
        if (data == null || data.length == 0) {
            return new ArrayList<>();
        }

        String ledgers = new String(data, StandardCharsets.UTF_8);
        List<Long> ledgerIds = new ArrayList<>();
        for (String ledgerId : ledgers.split(",")) {
            if (!ledgerId.isEmpty()) {
                ledgerIds.add(Long.parseLong(ledgerId));
            }
        }
        return ledgerIds;
    }

    // ---------------------------------- Logical Offset Management ---------------------------------- //
    /**
     * Gets the current logical offset for a topic partition.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return The current logical offset.
     * @throws Exception If an error occurs while retrieving the offset.
     */
    public long getPartitionLogicalOffset(String topic, int partition) throws Exception {
        String path = "/topics/" + topic + "/partitions/" + partition + "/logical_offset";
        ensurePathExists(path);

        byte[] data = zk.getData(path, false, null);
        return (data == null || data.length == 0) ? 0 : Long.parseLong(new String(data, StandardCharsets.UTF_8));
    }

    /**
     * Updates the logical offset for a topic partition.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param offset    The new logical offset.
     * @throws Exception If an error occurs while updating the offset.
     */
    public void setPartitionLogicalOffset(String topic, int partition, long offset) throws Exception {
        String path = "/topics/" + topic + "/partitions/" + partition + "/logical_offset";
        ensurePathExists(path);
        zk.setData(path, String.valueOf(offset).getBytes(StandardCharsets.UTF_8), -1);
    }

    // ---------------------------------- Consumer Offset Management ----------------------------------
    /**
     * Updates the last consumed offset for a consumer group.
     *
     * @param groupId   The consumer group ID.
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param offset    The offset to update.
     * @throws Exception If an error occurs while updating the offset.
     */
    public void updateConsumerOffset(String groupId, String topic, int partition, long offset) throws Exception {
        String path = "/consumers/" + groupId + "/" + topic + "/" + partition + "/offset";
        ensurePathExists(path);
        zk.setData(path, String.valueOf(offset).getBytes(StandardCharsets.UTF_8), -1);
    }

    /**
     * Gets the last consumed offset for a consumer group.
     *
     * @param groupId   The consumer group ID.
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return The last consumed offset.
     * @throws Exception If an error occurs while fetching the offset.
     */
    public long getConsumerOffset(String groupId, String topic, int partition) throws Exception {
        String path = "/consumers/" + groupId + "/" + topic + "/" + partition + "/offset";
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            return 0; // Default offset for a new consumer
        }
        byte[] data = zk.getData(path, false, null);
        return Long.parseLong(new String(data, StandardCharsets.UTF_8));
    }

    // ----------------------- Utility ----------------------- //

    /**
     * Closes the ZooKeeper connection.
     *
     * @throws InterruptedException If the thread is interrupted.
     */
    public void close() throws InterruptedException {
        zk.close();
    }
}
