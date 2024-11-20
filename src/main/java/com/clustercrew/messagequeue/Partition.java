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

        // Initialize a ledger for this partition in BookKeeper
        bkClient.createLedger(topic, partition);
    }

    /**
     * Append a message to the partition.
     *
     * @param message The message to append.
     * @throws Exception If an error occurs while appending.
     */
    public void appendMessage(Message message) throws Exception {
        bkClient.writeMessage(topic, partition, message);
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
}
