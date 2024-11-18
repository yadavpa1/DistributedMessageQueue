package com.clustercrew.messagequeue;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import com.clustercrew.messagequeue.MessageQueueOuterClass.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MessageQueueServer extends MessageQueueGrpc.MessageQueueImplBase {

    private final ZooKeeperClient zkClient;
    private final BookKeeperClient bkClient;
    private final Map<String, Map<Integer, Partition>> topicPartitions;

    public MessageQueueServer(String zkServers, String bkServers) {
        try {
            this.zkClient = new ZooKeeperClient(zkServers);
            this.bkClient = new BookKeeperClient(bkServers, zkClient);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MessageQueueServer", e);
        }
        this.topicPartitions = new HashMap<>();
    }    

    @Override
    public void produceMessage(ProduceMessageRequest request, StreamObserver<ProduceMessageResponse> responseObserver) {
        Message message = request.getMessage();
        String topic = message.getTopic();
        int partition = message.getPartition();

        try {
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

        Server server = ServerBuilder.forPort(8080)
                .addService(new MessageQueueServer(zkServers, bkServers))
                .build()
                .start();

        System.out.println("Message Queue Server started at port 8080");
        server.awaitTermination();
    }
}