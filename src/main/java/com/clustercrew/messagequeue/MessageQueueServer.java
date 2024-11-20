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
    private final Map<String, Map<Integer, Partition>> topicPartitions;
    private final String brokerId;
    private final PartitionAssigner partitionAssigner;

    public MessageQueueServer(String zkServers, String bkServers, String brokerId) {
        this.brokerId = brokerId;
        try {
            this.zkClient = new ZooKeeperClient(zkServers);
            this.bkClient = new BookKeeperClient(bkServers, zkClient);
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
        for (Integer partitionId : assignedPartitions) {
            getOrCreatePartition("my-topic", partitionId); // Assume "my-topic" for simplicity
        }
        System.out.println("Initialized partitions for broker: " + assignedPartitions);
    }

    @Override
    public void produceMessage(ProduceMessageRequest request, StreamObserver<ProduceMessageResponse> responseObserver) {
        Message message = request.getMessage();
        String topic = message.getTopic();
        int partition = message.getPartition();

        try {
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

            Partition partitionInstance = getPartition(topic, partition);
            if (partitionInstance == null) {
                throw new IllegalArgumentException("Partition not found");
            }

            var messages = partitionInstance.fetchMessages(startOffset, maxMessages);

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
    public void commitOffset(CommitOffsetRequest request, StreamObserver<CommitOffsetResponse> responseObserver) {
        String groupId = request.getGroupId();
        String topic = request.getTopic();
        int partition = request.getPartition();
        long offset = request.getOffset();

        try {
            zkClient.updateConsumerOffset(groupId, topic, partition, offset);

            CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
                    .setSuccess(true)
                    .build();
            responseObserver.onNext(response);
        } catch (Exception e) {
            CommitOffsetResponse response = CommitOffsetResponse.newBuilder()
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
        String bkServers = "localhost:3181";
        String brokerId = "broker-1";

        Server server = ServerBuilder.forPort(8080)
                .addService(new MessageQueueServer(zkServers, bkServers, brokerId))
                .build()
                .start();

        System.out.println("Message Queue Server started at port 8080");
        server.awaitTermination();
    }
}