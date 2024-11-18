package com.clustercrew.messagequeue;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ZooKeeperClient {
    private final ZooKeeper zk;

    public ZooKeeperClient(String zkServers) throws Exception {
        this.zk = new ZooKeeper(zkServers, 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("Connected to ZooKeeper");
            }
        });
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
            ensurePathExists(topicPath + "/" + i);
        }
        System.out.println("Topic created: " + topic);
    }

    /**
     * Stores the ledger mapping for a topic partition in ZooKeeper.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param ledgerId  The ID of the ledger to map.
     * @throws Exception If an error occurs while storing the mapping.
     */
    public void storeLedgerMapping(String topic, int partition, long ledgerId) throws Exception {
        String path = "/topics/" + topic + "/partitions/" + partition + "/ledger";
        ensurePathExists(path);
        zk.setData(path, String.valueOf(ledgerId).getBytes(StandardCharsets.UTF_8), -1);
    }

    /**
     * Retrieves the ledger ID for a topic partition from ZooKeeper.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return The ledger ID.
     * @throws Exception If an error occurs while retrieving the mapping.
     */
    public long getLedgerId(String topic, int partition) throws Exception {
        String path = "/topics/" + topic + "/partitions/" + partition + "/ledger";
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            throw new Exception("Ledger mapping not found for topic: " + topic + ", partition: " + partition);
        }
        byte[] data = zk.getData(path, false, null);
        return Long.parseLong(new String(data, StandardCharsets.UTF_8));
    }

    /**
     * Assigns a consumer group to a partition.
     *
     * @param groupId   The consumer group ID.
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param consumerId The consumer ID.
     * @throws Exception If an error occurs while assigning the consumer.
     */
    public void assignPartitionToConsumer(String groupId, String topic, int partition, String consumerId) throws Exception {
        String path = "/consumers/" + groupId + "/" + topic + "/" + partition + "/consumer";
        ensurePathExists(path);
        zk.setData(path, consumerId.getBytes(StandardCharsets.UTF_8), -1);
    }

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
            return 0; // Default offset
        }
        byte[] data = zk.getData(path, false, null);
        return Long.parseLong(new String(data, StandardCharsets.UTF_8));
    }

    /**
     * Gets the list of partitions for a topic.
     *
     * @param topic The topic name.
     * @return A list of partition IDs.
     * @throws Exception If an error occurs while fetching partitions.
     */
    public List<String> getPartitions(String topic) throws Exception {
        String topicPath = "/topics/" + topic;
        return zk.getChildren(topicPath, false);
    }

    /**
     * Closes the ZooKeeper connection.
     *
     * @throws InterruptedException If the thread is interrupted.
     */
    public void close() throws InterruptedException {
        zk.close();
    }
}
