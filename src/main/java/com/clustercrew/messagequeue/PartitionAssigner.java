package com.clustercrew.messagequeue;

import java.util.*;

public class PartitionAssigner {
    private final ZooKeeperClient zkClient;

    public PartitionAssigner(ZooKeeperClient zkClient) {
        this.zkClient = zkClient;
    }

    /**
     * Assigns partitions of a topic to the given list of brokers.
     * Uses a round-robin strategy to distribute partitions evenly.
     *
     * @param topic         The topic name.
     * @param numPartitions The total number of partitions for the topic.
     * @param brokerIds     List of active broker IDs.
     * @throws Exception If an error occurs during assignment.
     */
    public void assignPartitions(String topic, int numPartitions, List<String> brokerIds) throws Exception {
        if (brokerIds.isEmpty()) {
            throw new Exception("No active brokers available for partition assignment");
        }

        System.out.println("Assigning " + numPartitions + " partitions for topic: " + topic);

        for (int partition = 0; partition < numPartitions; partition++) {
            String brokerId = brokerIds.get(partition % brokerIds.size());
            zkClient.assignPartitionToBroker(topic, partition, brokerId);
            System.out.println("Partition " + partition + " assigned to broker " + brokerId);
        }
    }

    /**
     * Rebalances partitions of a topic across the given list of brokers.
     * Any partitions that are unassigned or previously assigned to inactive brokers
     * will be redistributed among the active brokers.
     *
     * @param topic     The topic name.
     * @param brokerIds List of active broker IDs.
     * @throws Exception If an error occurs during rebalancing.
     */
    public void rebalancePartitions(String topic, List<String> brokerIds) throws Exception {
        if (brokerIds.isEmpty()) {
            throw new Exception("No active brokers available for rebalancing.");
        }

        List<String> partitions = zkClient.getPartitions(topic);
        int numPartitions = partitions.size();

        System.out.println("Rebalancing " + numPartitions + " partitions for topic: " + topic);

        Map<Integer, String> currentAssignments = new HashMap<>();

        // Retrieve current broker assignments for each partition
        for (String partition : partitions) {
            int partitionId = Integer.parseInt(partition);
            try {
                String currentBroker = zkClient.getPartitionBroker(topic, partitionId);
                currentAssignments.put(partitionId, currentBroker);
            } catch (Exception e) {
                // If no broker is assigned, mark for reassignment
                currentAssignments.put(partitionId, null);
            }
        }

        // Reassign partitions using round-robin
        int brokerIndex = 0;
        for (Map.Entry<Integer, String> entry : currentAssignments.entrySet()) {
            int partitionId = entry.getKey();
            String assignedBroker = entry.getValue();

            if (assignedBroker == null || !brokerIds.contains(assignedBroker)) {
                // Assign partition to the next available broker
                String newBroker = brokerIds.get(brokerIndex % brokerIds.size());
                zkClient.assignPartitionToBroker(topic, partitionId, newBroker);
                System.out.println("Partition " + partitionId + " reassigned to broker " + newBroker);
                brokerIndex++;
            } else {
                System.out.println("Partition " + partitionId + " remains with broker " + assignedBroker);
            }
        }
    }

    /**
     * Retrieves the partition assignments for a topic.
     *
     * @param topic The topic name.
     * @return A map of partition IDs to broker IDs.
     * @throws Exception If an error occurs while fetching the assignments.
     */
    public Map<Integer, String> getPartitionAssignments(String topic) throws Exception {
        List<String> partitions = zkClient.getPartitions(topic);
        Map<Integer, String> assignments = new HashMap<>();

        for (String partition : partitions) {
            int partitionId = Integer.parseInt(partition);
            String brokerId = zkClient.getPartitionBroker(topic, partitionId);
            assignments.put(partitionId, brokerId);
        }

        return assignments;
    }

    /**
     * Prints the current partition assignments for debugging purposes.
     *
     * @param topic The topic name.
     * @throws Exception If an error occurs while fetching the assignments.
     */
    public void printPartitionAssignments(String topic) throws Exception {
        Map<Integer, String> assignments = getPartitionAssignments(topic);

        System.out.println("Partition Assignments for topic: " + topic);
        for (Map.Entry<Integer, String> entry : assignments.entrySet()) {
            System.out.println("Partition " + entry.getKey() + " -> Broker " + entry.getValue());
        }
    }
}
