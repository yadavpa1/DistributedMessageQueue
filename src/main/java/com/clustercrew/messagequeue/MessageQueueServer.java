package com.clustercrew.messagequeue;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import com.clustercrew.messagequeue.MessageQueueOuterClass.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageQueueServer extends MessageQueueGrpc.MessageQueueImplBase {

    private final ZooKeeperClient zkClient;
    private final BookKeeperClient bkClient;
    private final PartitionAssigner partitionAssigner;
    private final Map<String, Map<Integer, Partition>> topicPartitions;
    private final String brokerId;

    public MessageQueueServer(String zkServers, String bkServers, String brokerId) {
        this.brokerId = brokerId;
        try {
            this.zkClient = new ZooKeeperClient(zkServers);
            this.bkClient = new BookKeeperClient(zkServers, zkClient);
            this.partitionAssigner = new PartitionAssigner(zkClient);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MessageQueueServer", e);
        }
        this.topicPartitions = new HashMap<>();

        try {
            registerBroker();
            initializeAssignedPartitions();
        } catch (Exception e) {
            throw new RuntimeException("Failed to register broker or initialize partitions", e);
        }
    }
    
    /**
     * Registers the broker in ZooKeeper.
     */
    private void registerBroker() throws Exception {
        zkClient.registerBroker(brokerId);
        System.out.println("Broker registered with ID: " + brokerId);
    }

    /**
     * Initializes partitions assigned to this broker.
     */
    private void initializeAssignedPartitions() throws Exception {
        List<Integer> assignedPartitions = zkClient.getAssignedPartitions(brokerId);

        for (String topic : zkClient.getTopics()) {
            for (Integer partitionId : assignedPartitions) {
                if (!zkClient.partitionExists(topic, partitionId))
                    continue;
                getOrCreatePartition(topic, partitionId);
            }
        }

        System.out.println("Initialized partitions for broker: " + brokerId);
    }

    @Override
    public void produceMessage(ProduceMessageRequest request, StreamObserver<ProduceMessageResponse> responseObserver) {
        Message message = request.getMessage();
        String topic = message.getTopic();
        int partition = message.getPartition();

        try {
            // Check if the topic exists in ZooKeeper
            if (!zkClient.topicExists(topic)) {
                // Initialize topic metadata (e.g., partitions, retention, replicas)
                int numPartitions = 3;
                int retentionMs = 30 * 60 * 1000; // 30 mins retention
                int replicationFactor = 3;

                // Create the topic in ZooKeeper
                zkClient.createTopic(topic, numPartitions, retentionMs, replicationFactor);
                System.out.println("Topic created dynamically: " + topic);
            }

            // Validate if the partition exists
            if (!zkClient.partitionExists(topic, partition)) {
                throw new IllegalArgumentException("Partition " + partition + " does not exist for topic " + topic);
            }

            // Validate if this broker is responsible for the partition
            String assignedBroker = zkClient.getPartitionBroker(topic, partition);
            if (!assignedBroker.equals(brokerId)) {
                throw new IllegalArgumentException("Partition " + partition + " is not assigned to this broker.");
            }
            
            Partition partitionInstance = getOrCreatePartition(topic, partition);
            partitionInstance.appendMessage(message);

            ProduceMessageResponse response = ProduceMessageResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
        } catch (Exception e) {
            ProduceMessageResponse response = ProduceMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
            responseObserver.onNext(response);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void consumeMessage(ConsumeMessageRequest request, StreamObserver<ConsumeMessageResponse> responseObserver) {
        String groupId = request.getGroupId();
        String topic = request.getTopic();
        int partition = request.getPartition();
        long startOffset = request.getStartOffset();
        int maxMessages = request.getMaxMessages();

        try {
            // Validate if this broker is responsible for the partition
            String assignedBroker = zkClient.getPartitionBroker(topic, partition);
            if (!assignedBroker.equals(brokerId)) {
                throw new IllegalArgumentException("Partition " + partition + " is not assigned to this broker.");
            }

            // Check if max_messages is 0 (set offset mode)
            if (maxMessages == 0) {
                zkClient.updateConsumerOffset(groupId, topic, partition, startOffset);
                ConsumeMessageResponse response = ConsumeMessageResponse.newBuilder()
                        .setSuccess(true)
                        .build();
                responseObserver.onNext(response);
                return; // Skip normal message fetching
            }

            Partition partitionInstance = getPartition(topic, partition);
            if (partitionInstance == null) {
                throw new IllegalArgumentException(
                        "Partition not found for topic " + topic + " and partition " + partition);
            }

            List<Message> messages = partitionInstance.fetchMessages(startOffset, maxMessages);

            // Update consumer offset for the group
            long newOffset = startOffset + messages.size();
            zkClient.updateConsumerOffset(groupId, topic, partition, newOffset);

            ConsumeMessageResponse.Builder responseBuilder = ConsumeMessageResponse.newBuilder()
                    .setSuccess(true);
            
            for (Message msg : messages) {
                responseBuilder.addMessages(msg);
            }

            responseObserver.onNext(responseBuilder.build());
        } catch (Exception e) {
            ConsumeMessageResponse response = ConsumeMessageResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
            responseObserver.onNext(response);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getMetadata(MetadataRequest request, StreamObserver<MetadataResponse> responseObserver) {
        String topic = request.getTopic();

        try {
            if (!zkClient.topicExists(topic)) {
                throw new IllegalArgumentException("Topic " + topic + " does not exist.");
            }

            List<String> partitions = zkClient.getPartitions(topic);
            MetadataResponse.Builder responseBuilder = MetadataResponse.newBuilder()
                    .setSuccess(true);

            for (String partition : partitions) {
                int partitionId = Integer.parseInt(partition);
                String brokerAddress = zkClient.getPartitionBroker(topic, partitionId);

                PartitionMetadata partitionMetadata = PartitionMetadata.newBuilder()
                        .setPartitionId(partitionId)
                        .setBrokerAddress(brokerAddress)
                        .build();

                responseBuilder.addPartitions(partitionMetadata);
            }

            responseObserver.onNext(responseBuilder.build());
        } catch (Exception e) {
            MetadataResponse response = MetadataResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
            responseObserver.onNext(response);
        } finally {
            responseObserver.onCompleted();
        }
    }

    /**
     * Fetches or creates a partition instance.
     *
     * @param topic     The topic name.
     * @param partition The partition ID.
     * @return The Partition instance.
     * @throws Exception If an error occurs while creating the partition.
     */
    private Partition getOrCreatePartition(String topic, int partition) throws Exception {
        synchronized (topicPartitions) {
            topicPartitions.computeIfAbsent(topic, k -> new HashMap<>());

            return topicPartitions.get(topic).computeIfAbsent(partition, p -> {
                try {
                    return new Partition(zkClient, bkClient, topic, partition);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private Partition getPartition(String topic, int partition) {
        synchronized (topicPartitions) {
            if (!topicPartitions.containsKey(topic)) {
                return null;
            }
            return topicPartitions.get(topic).get(partition);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String zkServers = "localhost:2181";
        String bkServers = "";
        String brokerId = "broker-1";

        Server server = ServerBuilder.forPort(8080)
                .addService(new MessageQueueServer(zkServers, bkServers, brokerId))
                .build()
                .start();

        System.out.println("Message Queue Server started at port 8080");
        server.awaitTermination();
    }
}