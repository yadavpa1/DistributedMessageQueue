package com.clustercrew.messagequeue;

import com.clustercrew.messagequeue.MessageQueueOuterClass.Message;
import java.util.List;

public class Partition {
    private final ZooKeeperClient zkClient;
    private final BookKeeperClient bkClient;
    private final String topic;
    private final int partition;

    public Partition(ZooKeeperClient zkClient, BookKeeperClient bkClient, String topic, int partition) throws Exception {
        this.zkClient = zkClient;
        this.bkClient = bkClient;
        this.topic = topic;
        this.partition = partition;

        // Ensure an active ledger exists
        bkClient.getOrCreateActiveLedger(topic, partition);

        // Initialize logical offset if not already done
        if (zkClient.getPartitionLogicalOffset(topic, partition) == 0) {
            zkClient.setPartitionLogicalOffset(topic, partition, 0);
        }
    }

    /**
     * Append a message to the partition.
     *
     * @param message The message to append.
     * @throws Exception If an error occurs while appending.
     */
    public void appendMessage(Message message) throws Exception {
       // Write the message to the active ledger
        bkClient.writeMessage(topic, partition, message);

        // Increment the logical offset after appending
        long currentOffset = zkClient.getPartitionLogicalOffset(topic, partition);
        zkClient.setPartitionLogicalOffset(topic, partition, currentOffset + 1);

        System.out.println("Message appended to partition: " + topic + " - " + partition);
    }

    /**
     * Fetch messages from the partition starting from the given offset.
     *
     * @param startOffset The offset to start fetching from.
     * @param maxMessages The maximum number of messages to fetch.
     * @return A list of messages.
     * @throws Exception If an error occurs while fetching.
     */
    public List<Message> fetchMessages(long startOffset, int maxMessages) throws Exception {
        return bkClient.readMessages(topic, partition, startOffset, maxMessages);
    }

    /**
     * Retrieves the current logical offset for this partition.
     *
     * @return The current logical offset.
     * @throws Exception If an error occurs while retrieving the offset.
     */
    public long getLogicalOffset() throws Exception {
        return zkClient.getPartitionLogicalOffset(topic, partition);
    }
}
