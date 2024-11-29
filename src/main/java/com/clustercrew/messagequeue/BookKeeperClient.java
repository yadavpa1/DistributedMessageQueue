package com.clustercrew.messagequeue;

import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.conf.ClientConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.clustercrew.messagequeue.MessageQueueOuterClass.Message;

public class BookKeeperClient {
    private final BookKeeper bookKeeper;
    private final ZooKeeperClient zkClient;

    // Active ledgers map: topic -> partition -> ledger handle
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, LedgerHandle>> activeLedgers;

    // Lock map to synchronize ledger creation for a topic partition
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, ReentrantLock>> ledgerLocks;

    public BookKeeperClient(String servers, ZooKeeperClient zkClient) throws Exception {
        ClientConfiguration config = new ClientConfiguration();
        config.setMetadataServiceUri("zk+null://127.0.0.1:2181/ledgers");
        this.bookKeeper = BookKeeper.forConfig(config).build();
        this.zkClient = zkClient;
        this.activeLedgers = new ConcurrentHashMap<>();
        this.ledgerLocks = new ConcurrentHashMap<>();
        System.out.println("Connected to BookKeeper!");
    }

    /**
     * Creates a new ledger for a topic partition and updates the mapping in ZooKeeper.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return The newly created ledger handle.
     * @throws Exception If an error occurs while creating the ledger.
     */
    private LedgerHandle createNewLedger(String topic, int partition) throws Exception {
        LedgerHandle ledger = bookKeeper.createLedger(
                BookKeeper.DigestType.CRC32,
                "password".getBytes(StandardCharsets.UTF_8));

        long ledgerId = ledger.getId();
        // Update ledger mapping in ZooKeeper
        zkClient.addLedgerToPartition(topic, partition, ledgerId);

        System.out.println("Ledger created for topic: " + topic + ", partition: " + partition + ", ID: " + ledgerId);
        return ledger;
    }

    /**
     * Ensures there is an active ledger for the given topic partition.
     * If no ledger exists or the current ledger is closed, a new ledger is created.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @return The active ledger handle for the partition.
     * @throws Exception If an error occurs while creating or fetching the ledger.
     */
    public LedgerHandle getOrCreateActiveLedger(String topic, int partition) throws Exception {
        activeLedgers.putIfAbsent(topic, new ConcurrentHashMap<>());
        ledgerLocks.putIfAbsent(topic, new ConcurrentHashMap<>());
        ledgerLocks.get(topic).putIfAbsent(partition, new ReentrantLock());

        ReentrantLock lock = ledgerLocks.get(topic).get(partition);

        lock.lock();
        try {
            LedgerHandle ledger = activeLedgers.get(topic).get(partition);

            // If no ledger exists or it is closed, create a new ledger
            if (ledger == null || ledger.isClosed()) {
                ledger = createNewLedger(topic, partition);
                activeLedgers.get(topic).put(partition, ledger);
            }
            return ledger;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Writes a message to the current active ledger of the topic partition.
     * If the ledger reaches its max entries, it is closed, and a new ledger is created.
     *
     * @param topic     The topic name.
     * @param partition The partition number.
     * @param message   The message to write.
     * @throws Exception If an error occurs while writing the message.
     */
    public void writeMessage(String topic, int partition, Message message) throws Exception {
        LedgerHandle ledger;

        while (true) {
            ledger = getOrCreateActiveLedger(topic, partition);

            byte[] serializedMessage = message.toByteArray();
            try {
                ledger.addEntry(serializedMessage);
                System.out.println("Message written to topic: " + topic + ", partition: " + partition);
                break; // Successful write, exit loop
            } catch (BKLedgerClosedException e) {
                System.out.println("Ledger closed for topic: " + topic + ", partition: " + partition);
                // Remove the closed ledger from active map and retry
                activeLedgers.get(topic).remove(partition);
            }
        }
    }

    /**
     * Reads messages from a topic partition ledger starting from the specified logical offset.
     *
     * @param topic       The topic name.
     * @param partition   The partition number.
     * @param startOffset The logical offset to start reading from.
     * @param maxMessages The maximum number of messages to fetch.
     * @return A list of messages.
     * @throws Exception If an error occurs while reading the messages.
     */
    public List<Message> readMessages(String topic, int partition, long startOffset, int maxMessages) throws Exception {
        List<Message> messages = new ArrayList<>();

        List<Long> ledgerIds = zkClient.getPartitionLedgers(topic, partition);
        long currentOffset = 0;

        for (int i = 0; i < ledgerIds.size(); i++) {
            long ledgerId = ledgerIds.get(i);
            boolean isActiveLedger = (i == ledgerIds.size() - 1); // The last ledger is the active one

            try (LedgerHandle ledger = isActiveLedger 
                    ? getOrCreateActiveLedger(topic, partition) 
                    : bookKeeper.openLedger(
                      ledgerId,
                      BookKeeper.DigestType.CRC32,
                      "password".getBytes(StandardCharsets.UTF_8))) {

                long ledgerStart = currentOffset;
                long ledgerEnd = currentOffset + ledger.getLastAddConfirmed() + 1;

                if (startOffset < ledgerEnd) {
                    long offsetInLedger = Math.max(0, startOffset - ledgerStart);
                    long endOffsetInLedger = Math.min(ledger.getLastAddConfirmed(), offsetInLedger + maxMessages - 1);

                    Enumeration<LedgerEntry> entries = ledger.readEntries(offsetInLedger, endOffsetInLedger);
                    while (entries.hasMoreElements()) {
                        LedgerEntry entry = entries.nextElement();
                        Message message = Message.parseFrom(entry.getEntry());
                        messages.add(message);

                        if (messages.size() >= maxMessages) {
                            break;
                        }
                    }
                }

                currentOffset = ledgerEnd;
                if (messages.size() >= maxMessages) {
                    break;
                }
            
            } catch (BKLedgerClosedException e) {
                System.out.println("Ledger closed unexpectedly while reading: " + ledgerId);
                throw new Exception("Error reading ledger " + ledgerId, e);
            }
        }

        System.out.println("Read " + messages.size() + " messages from topic: " + topic + ", partition: " + partition);
        return messages;
    }

    /**
     * Closes the BookKeeper client and all active ledgers.
     *
     * @throws Exception If an error occurs while closing the client or ledgers.
     */
    public void close() throws Exception {
        for (Map<Integer, LedgerHandle> partitionMap : activeLedgers.values()) {
            for (LedgerHandle ledger : partitionMap.values()) {
                if (ledger != null && !ledger.isClosed()) {
                    ledger.close();
                }
            }
        }
        bookKeeper.close();
        System.out.println("BookKeeper client connection closed.");
    }
}