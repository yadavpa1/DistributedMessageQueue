package com.clustercrew.messagequeue;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ClientConfiguration;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import com.clustercrew.messagequeue.MessageQueueOuterClass.Message;

public class BookKeeperClient {
    private final BookKeeper bookKeeper;

    public BookKeeperClient(String bkServers) throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        config.setMetadataServiceUri("zk+null://" + bkServers + "/ledgers");
        this.bookKeeper = BookKeeper.forConfig(config).build();
        System.out.println("Connected to BookKeeper: " + bkServers);
    }

    /**
     * Creates a new ledger for a topic partition.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @throws Exception If an error occurs while creating the ledger.
     */
    public void createLedger(String topic, int partition) throws Exception {
        try (LedgerHandle ledger = bookKeeper.createLedger(
                BookKeeper.DigestType.CRC32,
                "password".getBytes(StandardCharsets.UTF_8))) {
            System.out.println("Ledger created for topic: " + topic + ", partition: " + partition + ", ID: " + ledger.getId());
        }
    }

    /**
     * Writes a message to the specified topic partition ledger.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param message   The message to write.
     * @throws Exception If an error occurs while writing the message.
     */
    public void writeMessage(String topic, int partition, Message message) throws Exception {
        try (LedgerHandle ledger = bookKeeper.openLedger(
                generateLedgerId(topic, partition),
                BookKeeper.DigestType.CRC32,
                "password".getBytes(StandardCharsets.UTF_8))) {
            byte[] serializedMessage = message.toByteArray();
            ledger.addEntry(serializedMessage);
            System.out.println("Message written to topic: " + topic + ", partition: " + partition);
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
        try (LedgerHandle ledger = bookKeeper.openLedger(
                generateLedgerId(topic, partition),
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
     * Generates a unique ID for a ledger based on the topic and partition.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return The generated ledger ID.
     */
    private int generateLedgerId(String topic, int partition) {
        return (topic + ":" + partition).hashCode();
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