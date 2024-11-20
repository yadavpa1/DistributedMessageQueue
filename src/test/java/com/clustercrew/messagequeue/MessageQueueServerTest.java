// package com.clustercrew.messagequeue;

// import io.grpc.ManagedChannel;
// import io.grpc.ManagedChannelBuilder;
// import com.clustercrew.messagequeue.MessageQueueOuterClass.Message;
// import com.clustercrew.messagequeue.MessageQueueOuterClass.*;

// import org.junit.AfterClass;
// import org.junit.BeforeClass;
// import org.junit.Test;

// import static org.junit.Assert.*;

// import java.util.List;

// public class MessageQueueServerTest {

//     private static ManagedChannel channel;
//     private static MessageQueueGrpc.MessageQueueBlockingStub blockingStub;
//     private static MessageQueueServer server;

//     @BeforeClass
//     public static void setUp() throws Exception {
//         // Start the server
//         String zkServers = "localhost:2181";
//         String bkServers = "localhost:3181";
//         server = new MessageQueueServer(zkServers, bkServers);
//         new Thread(() -> {
//             try {
//                 server.main(new String[0]);
//             } catch (Exception e) {
//                 e.printStackTrace();
//             }
//         }).start();

//         // Set up the client channel
//         channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext().build();
//         blockingStub = MessageQueueGrpc.newBlockingStub(channel);

//         // Wait for the server to initialize
//         Thread.sleep(1000);
//     }

//     @AfterClass
//     public static void tearDown() throws Exception {
//         // Shut down the client channel
//         channel.shutdownNow();

//         // Server shutdown (implement shutdown logic if needed)
//         System.out.println("Tests completed.");
//     }

//     @Test
//     public void testProduceAndConsumeMessage() {
//         // Create a message
//         Message message = Message.newBuilder()
//                 .setKey("testKey")
//                 .setValue("testValue")
//                 .setTopic("testTopic")
//                 .setPartition(0)
//                 .setOffset(0)
//                 .setTimestamp(System.currentTimeMillis())
//                 .build();

//         // Produce the message
//         ProduceMessageRequest produceRequest = ProduceMessageRequest.newBuilder()
//                 .setMessage(message)
//                 .setProducerId("producer1")
//                 .setAckMode("1")
//                 .build();
//         ProduceMessageResponse produceResponse = blockingStub.produceMessage(produceRequest);

//         assertTrue(produceResponse.getSuccess());
//         System.out.println("Produced message successfully.");

//         // Consume the message
//         ConsumeMessageRequest consumeRequest = ConsumeMessageRequest.newBuilder()
//                 .setGroupId("testGroup")
//                 .setTopic("testTopic")
//                 .setPartition(0)
//                 .setStartOffset(0)
//                 .setMaxMessages(1)
//                 .build();
//         ConsumeMessageResponse consumeResponse = blockingStub.consumeMessage(consumeRequest);

//         assertTrue(consumeResponse.getSuccess());
//         List<Message> messages = consumeResponse.getMessagesList();
//         assertEquals(1, messages.size());

//         Message consumedMessage = messages.get(0);
//         assertEquals("testKey", consumedMessage.getKey());
//         assertEquals("testValue", consumedMessage.getValue());
//         System.out.println("Consumed message successfully.");
//     }

//     @Test
//     public void testCommitOffset() {
//         // Commit an offset
//         CommitOffsetRequest commitRequest = CommitOffsetRequest.newBuilder()
//                 .setGroupId("testGroup")
//                 .setTopic("testTopic")
//                 .setPartition(0)
//                 .setOffset(1)
//                 .build();
//         CommitOffsetResponse commitResponse = blockingStub.commitOffset(commitRequest);

//         assertTrue(commitResponse.getSuccess());
//         System.out.println("Offset committed successfully.");
//     }
// }