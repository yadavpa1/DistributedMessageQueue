package com.clustercrew.messagequeue;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import com.clustercrew.messagequeue.MessageQueueOuterClass.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageQueueServer extends MessageQueueGrpc.MessageQueueImplBase {

    private final ZooKeeperClient zkClient;
    private final BookKeeperClient bkClient;
    private final Map<String, Map<Integer, Partition>> topicPartitions;
    private final String brokerId;
    private final String brokerAddress;

    public MessageQueueServer(String zkServers, String bkServers, String brokerId, String brokerAddress) {
        this.brokerId = brokerId;
        this.brokerAddress = brokerAddress;
        this.topicPartitions = new HashMap<>();

        try {
            this.zkClient = new ZooKeeperClient(zkServers);
            this.bkClient = new BookKeeperClient(zkServers, zkClient);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MessageQueueServer", e);
        }

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
        zkClient.registerBroker(brokerId, brokerAddress);
        System.out.println("Broker registered with ID: " + brokerId + " and address: " + brokerAddress);
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
    public void produceMessages(ProduceMessagesRequest request, StreamObserver<ProduceMessagesResponse> responseObserver) {
        List<Message> messages = request.getMessagesList();

        // Group messages by topic and partition
        Map<String, Map<Integer, List<Message>>> groupedMessages = new HashMap<>();
        for (Message message : messages) {
            groupedMessages
                .computeIfAbsent(message.getTopic(), k -> new HashMap<>())
                .computeIfAbsent(message.getPartition(), k -> new ArrayList<>())
                .add(message);
        }

        try {
            // Process each group of messages for the same topic and partition
            for (Map.Entry<String, Map<Integer, List<Message>>> topicEntry : groupedMessages.entrySet()) {
                String topic = topicEntry.getKey();

                for (Map.Entry<Integer, List<Message>> partitionEntry : topicEntry.getValue().entrySet()) {
                    int partition = partitionEntry.getKey();
                    List<Message> partitionMessages = partitionEntry.getValue();

                    // Validate if this broker is responsible for the partition
                    String assignedBroker = zkClient.getPartitionBroker(topic, partition);
                    if (!assignedBroker.equals(brokerId)) {
                        throw new IllegalArgumentException(
                                "Partition " + partition + " is not assigned to this broker.");
                    }

                    Partition partitionInstance = getOrCreatePartition(topic, partition);
                    partitionInstance.appendMessagesBatch(partitionMessages);
                }
                
                ProduceMessagesResponse response = ProduceMessagesResponse.newBuilder()
                        .setSuccess(true)
                        .build();
                responseObserver.onNext(response);
            }            
        } catch (Exception e) {
            ProduceMessagesResponse response = ProduceMessagesResponse.newBuilder()
                    .setSuccess(false)
                    .setErrorMessage(e.getMessage())
                    .build();
            responseObserver.onNext(response);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void consumeMessages(ConsumeMessagesRequest request, StreamObserver<ConsumeMessagesResponse> responseObserver) {
        String groupId = request.getGroupId();
        String topic = request.getTopic();
        int partition = request.getPartition();
        long startOffset = request.getStartOffset();
        int maxMessages = request.getMaxMessages();

        try {
            // Check if max_messages is 0 (set offset mode)
            if (maxMessages == 0) {
                zkClient.updateConsumerOffset(groupId, topic, partition, startOffset);
                ConsumeMessagesResponse response = ConsumeMessagesResponse.newBuilder()
                        .setSuccess(true)
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return; // Skip normal message fetching
            }

            // Validate if this broker is responsible for the partition
            String assignedBroker = zkClient.getPartitionBroker(topic, partition);
            if (!assignedBroker.equals(brokerId)) {
                throw new IllegalArgumentException("Partition " + partition + " is not assigned to this broker.");
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

            ConsumeMessagesResponse.Builder responseBuilder = ConsumeMessagesResponse.newBuilder()
                    .setSuccess(true)
                    .addAllMessages(messages);

            responseObserver.onNext(responseBuilder.build());
        } catch (Exception e) {
            ConsumeMessagesResponse response = ConsumeMessagesResponse.newBuilder()
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
            // Create topic and partitions if the topic does not exist
            if (!zkClient.topicExists(topic)) {
                // Initialize topic metadata (e.g., partitions, retention, replicas)
                int numPartitions = 3;
                int retentionMs = 30 * 60 * 1000; // 30 mins retention
                int replicationFactor = 3;

                // Create the topic in ZooKeeper
                zkClient.createTopic(topic, numPartitions, retentionMs, replicationFactor);
                System.out.println("Topic created dynamically: " + topic);
            }

            List<String> partitions = zkClient.getPartitions(topic);
            MetadataResponse.Builder responseBuilder = MetadataResponse.newBuilder()
                    .setSuccess(true);

            for (String partition : partitions) {
                int partitionId = Integer.parseInt(partition);
                String brokerId = zkClient.getPartitionBroker(topic, partitionId);
                String brokerAddress = zkClient.getBrokerAddress(brokerId);

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
        String brokerAddress = "localhost:8080";

        Server server = ServerBuilder.forPort(8080)
                .addService(new MessageQueueServer(zkServers, bkServers, brokerId, brokerAddress))
                .build()
                .start();

        System.out.println("Message Queue Server started at port 8080");
        server.awaitTermination();
    }
}