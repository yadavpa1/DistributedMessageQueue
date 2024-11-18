package com.clustercrew.messagequeue;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import com.clustercrew.messagequeue.MessageQueueOuterClass.Message;

public class BookKeeperClient {
    private final BookKeeper bookKeeper;
    private final ZooKeeperClient zkClient;

    public BookKeeperClient(String bkServers, ZooKeeperClient zkClient) throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        config.setMetadataServiceUri("zk+null://" + bkServers + "/ledgers");
        this.bookKeeper = BookKeeper.forConfig(config).build();
        this.zkClient = zkClient;
        System.out.println("Connected to BookKeeper: " + bkServers);
    }

    /**
     * Creates a new ledger for a topic partition and updates the mapping in ZooKeeper.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return The ID of the newly created ledger.
     * @throws Exception If an error occurs while creating the ledger.
     */
    public long createLedger(String topic, int partition) throws Exception {
        try (LedgerHandle ledger = bookKeeper.createLedger(
                BookKeeper.DigestType.CRC32,
                "password".getBytes(StandardCharsets.UTF_8))) {

            long ledgerId = ledger.getId();
            // Update ledger mapping in ZooKeeper
            zkClient.storeLedgerMapping(topic, partition, ledgerId);

            System.out.println("Ledger created for topic: " + topic + ", partition: " + partition + ", ID: " + ledgerId);
            return ledgerId;
        }
    }

    /**
     * Writes a message to the current ledger of the topic partition.
     * If the ledger is full, a new ledger is created, and the mapping is updated.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param message   The message to write.
     * @throws Exception If an error occurs while writing the message.
     */
    public void writeMessage(String topic, int partition, Message message) throws Exception {
        while (true) {
            long ledgerId = zkClient.getLedgerId(topic, partition);

            try (LedgerHandle ledger = bookKeeper.openLedger(
                    ledgerId,
                    BookKeeper.DigestType.CRC32,
                    "password".getBytes(StandardCharsets.UTF_8))) {

                byte[] serializedMessage = message.toByteArray();
                try {
                    ledger.addEntry(serializedMessage);
                    System.out.println("Message written to topic: " + topic + ", partition: " + partition);
                    break; // Successful write, exit loop
                } catch (BKLedgerClosedException e) {
                    System.out.println("Ledger full for topic: " + topic + ", partition: " + partition);
                    // Create a new ledger and retry
                    long newLedgerId = createLedger(topic, partition);
                    zkClient.storeLedgerMapping(topic, partition, newLedgerId);
                }
            }
        }
    }

    /**
     * Reads messages from a topic partition ledger starting from the specified offset.
     *
     * @param topic       The topic name.
     * @param partition   The partition number.
     * @param startOffset The offset to start reading from.
     * @param maxMessages The maximum number of messages to fetch.
     * @return A list of messages.
     * @throws Exception If an error occurs while reading the messages.
     */
    public List<Message> readMessages(String topic, int partition, long startOffset, int maxMessages) throws Exception {
        List<Message> messages = new ArrayList<>();
        long ledgerId = zkClient.getLedgerId(topic, partition);

        try (LedgerHandle ledger = bookKeeper.openLedger(
                ledgerId,
                BookKeeper.DigestType.CRC32,
                "password".getBytes(StandardCharsets.UTF_8))) {
            long endOffset = Math.min(startOffset + maxMessages - 1, ledger.getLastAddConfirmed());
            Enumeration<LedgerEntry> entries = ledger.readEntries(startOffset, endOffset);

            while (entries.hasMoreElements()) {
                LedgerEntry entry = entries.nextElement();
                Message message = Message.parseFrom(entry.getEntry());
                messages.add(message);
            }
            System.out.println("Read " + messages.size() + " messages from topic: " + topic + ", partition: " + partition);
        }
        return messages;
    }

    /**
     * Closes the BookKeeper client.
     *
     * @throws Exception If an error occurs while closing the client.
     */
    public void close() throws Exception {
        bookKeeper.close();
        System.out.println("BookKeeper client connection closed.");
    }
}