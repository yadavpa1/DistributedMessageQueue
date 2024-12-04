package com.clustercrew.messagequeue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MessageQueueCluster {

    public static void main(String[] args) {
        int numberOfServers = 3; // Number of servers to start
        int startingPort = 8080; // Starting port for servers
        String zkServers = "localhost:2181";
        String bkServers = "";
        List<Process> processes = new ArrayList<>();

        try {
            // Start multiple server processes
            for (int i = 0; i < numberOfServers; i++) {
                String brokerId = "broker-" + (i + 1);
                String brokerAddress = "localhost:" + (startingPort + i);

                // Command to start a server process
                List<String> command = new ArrayList<>();
                command.add("java");
                command.add("-cp");
                command.add(System.getProperty("java.class.path")); // Include the current classpath
                command.add("com.clustercrew.messagequeue.MessageQueueServer");
                command.add(zkServers);
                command.add(bkServers);
                command.add(brokerId);
                command.add(brokerAddress);

                // Start the process
                ProcessBuilder processBuilder = new ProcessBuilder(command);
                processBuilder.inheritIO(); // To inherit console output
                Process process = processBuilder.start();
                processes.add(process);

                System.out.println("Started Message Queue Server with Broker ID: " + brokerId + " on port " + (startingPort + i));
            }

            // Wait for all servers to run indefinitely
            System.out.println("Servers are running. Use CTRL+C to stop.");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                for (Process process : processes) {
                    process.destroy();
                }
                System.out.println("All servers have been stopped.");
            }));

            // Block the main thread
            Thread.currentThread().join();

        } catch (IOException e) {
            System.err.println("Failed to start server processes: " + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println("Cluster interrupted.");
        }
    }
}
