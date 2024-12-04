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

        // Calculate current load on each broker
        Map<String, Integer> brokerLoad = calculateBrokerLoad(brokerIds);

        // Priority queue to pick brokers with the least load
        PriorityQueue<String> brokerQueue = new PriorityQueue<>(Comparator.comparingInt(brokerLoad::get));
        brokerQueue.addAll(brokerIds);

        for (int partition = 0; partition < numPartitions; partition++) {
            // Pick the broker with the least load
            String brokerId = brokerQueue.poll();
            zkClient.assignPartitionToBroker(topic, partition, brokerId);

            // Update the broker's load and reinsert into the queue
            brokerLoad.put(brokerId, brokerLoad.get(brokerId) + 1);
            brokerQueue.offer(brokerId);

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

        // Calculate current load on each broker
        Map<String, Integer> brokerLoad = calculateBrokerLoad(brokerIds);

        // Priority queue to pick brokers with the least load
        PriorityQueue<String> brokerQueue = new PriorityQueue<>(Comparator.comparingInt(brokerLoad::get));
        brokerQueue.addAll(brokerIds);

        for (String partitionStr : partitions) {
            int partition = Integer.parseInt(partitionStr);

            // Remove the partition's current assignment
            String currentBroker = zkClient.getPartitionBroker(topic, partition);
            if (brokerLoad.containsKey(currentBroker)) {
                brokerLoad.put(currentBroker, brokerLoad.get(currentBroker) - 1);
                brokerQueue.remove(currentBroker);
                brokerQueue.offer(currentBroker);
            }

            // Assign the partition to the broker with the least load
            String newBroker = brokerQueue.poll();
            zkClient.assignPartitionToBroker(topic, partition, newBroker);

            // Update the broker's load and reinsert into the queue
            brokerLoad.put(newBroker, brokerLoad.get(newBroker) + 1);
            brokerQueue.offer(newBroker);

            System.out.println("Partition " + partition + " reassigned to broker " + newBroker);

        }
    }

    /**
     * Calculates the current load (number of partitions assigned) on each broker.
     *
     * @param brokerIds List of active broker IDs.
     * @return A map of broker IDs to their current load.
     * @throws Exception If an error occurs while retrieving broker assignments.
     */
    private Map<String, Integer> calculateBrokerLoad(List<String> brokerIds) throws Exception {
        Map<String, Integer> brokerLoad = new HashMap<>();

        for (String brokerId : brokerIds) {
            List<Integer> partitions = zkClient.getAssignedPartitions(brokerId);
            brokerLoad.put(brokerId, partitions.size());
        }

        return brokerLoad;
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
