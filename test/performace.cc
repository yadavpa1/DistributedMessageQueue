#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include "producer.h"
#include "consumer_group.h"
#include "chrono"

using namespace std;

// Utility function to generate random messages
string GenerateRandomString(size_t length) {
    static const string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> dis(0, charset.size() - 1);
    string result;
    for (size_t i = 0; i < length; ++i) {
        result += charset[dis(gen)];
    }
    return result;
}

// Test case 1: Single Producer, Single Consumer Group
void TestCase1() {
    cout << "Running TestCase1: Single Producer, Single Consumer Group" << endl;

    vector<string> bootstrap_servers = {"localhost:8080"};
    Producer producer(bootstrap_servers, 1000, 500, "producer1");
    ConsumerGroup consumerGroup("tag1", "group1");

    consumerGroup.AddConsumer(bootstrap_servers, "consumer1", {"test_topic"}, {0}, {0});

    const int totalMessages = 10000;
    auto startTime = chrono::high_resolution_clock::now();

    // Producer sends messages
    for (int i = 0; i < totalMessages; ++i) {
        producer.ProduceMessage("key" + to_string(i), GenerateRandomString(20), "test_topic");
    }

    // Consumer fetches messages
    vector<MessageResponse> responses = consumerGroup.ConsumeMessage("test_topic", 0, totalMessages);

    auto endTime = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = endTime - startTime;

    cout << "TestCase1 Results: " << endl;
    cout << "Total messages: " << totalMessages << endl;
    cout << "Elapsed time: " << elapsed.count() << " seconds" << endl;
    cout << "Throughput: " << totalMessages / elapsed.count() << " messages/second" << endl;
}

// Test case 2: Multiple Producers, Single Consumer Group (Using Processes)
void TestCase2WithProcesses() {
    cout << "Running TestCase2: Multiple Producers, Single Consumer Group (Using Processes)" << endl;

    vector<string> bootstrap_servers = {"localhost:8080"};
    ConsumerGroup consumerGroup("tag2", "group2");
    consumerGroup.AddConsumer(bootstrap_servers, "consumer1", {"test_topic"}, {0}, {0});

    const int messagesPerProducer = 5000;
    const int numProducers = 3;

    // Fork processes for producers
    for (int i = 0; i < numProducers; ++i) {
        pid_t pid = fork();
        if (pid == 0) { // Child process
            Producer producer(bootstrap_servers, 1000, 500, "producer" + to_string(i));
            for (int j = 0; j < messagesPerProducer; ++j) {
                producer.ProduceMessage("key" + to_string(i) + "_" + to_string(j),
                                        GenerateRandomString(20), "test_topic");
            }
            exit(0); // Exit the child process
        } else if (pid < 0) {
            cerr << "Error forking process for producer " << i << endl;
            exit(1);
        }
    }

    // Wait for all child processes to finish
    for (int i = 0; i < numProducers; ++i) {
        int status;
        wait(&status);
    }

    // Consumer fetches messages
    vector<MessageResponse> responses = consumerGroup.ConsumeMessage("test_topic", 0, numProducers * messagesPerProducer);

    cout << "TestCase2 Results: " << endl;
    cout << "Total messages produced: " << numProducers * messagesPerProducer << endl;
    cout << "Messages consumed: " << responses.size() << endl;
}

// Test case 3: High Volume Stress Test (Using Processes)
void TestCase3WithProcesses() {
    cout << "Running TestCase3: High Volume Stress Test (Using Processes)" << endl;

    vector<string> bootstrap_servers = {"localhost:8080"};
    vector<Producer> producers;
    ConsumerGroup consumerGroup("tag3", "group3");

    // Adding consumers
    for (int i = 0; i < 5; ++i) {
        consumerGroup.AddConsumer(bootstrap_servers, "consumer" + to_string(i), {"test_topic"}, {i}, {0});
    }

    const int messagesPerProducer = 100000;
    const int numProducers = 10;

    // Fork processes for producers
    for (int i = 0; i < numProducers; ++i) {
        pid_t pid = fork();
        if (pid == 0) { // Child process
            Producer producer(bootstrap_servers, 1000, 500, "producer" + to_string(i));
            for (int j = 0; j < messagesPerProducer; ++j) {
                producer.ProduceMessage("key" + to_string(i) + "_" + to_string(j),
                                        GenerateRandomString(20), "test_topic");
            }
            exit(0); // Exit the child process
        } else if (pid < 0) {
            cerr << "Error forking process for producer " << i << endl;
            exit(1);
        }
    }

    // Wait for all child processes to finish
    for (int i = 0; i < numProducers; ++i) {
        int status;
        wait(&status);
    }

    cout << "All producers completed." << endl;

    // Simulate consumers fetching messages
    auto startTime = chrono::high_resolution_clock::now();

    vector<MessageResponse> responses = consumerGroup.ConsumeMessage("test_topic", 0, numProducers * messagesPerProducer);

    auto endTime = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = endTime - startTime;

    cout << "TestCase3 Results: " << endl;
    cout << "Total messages consumed: " << responses.size() << endl;
    cout << "Elapsed time: " << elapsed.count() << " seconds" << endl;
    cout << "Throughput: " << (numProducers * messagesPerProducer) / elapsed.count() << " messages/second" << endl;
}

// Main function to execute all test cases
int main() {
    TestCase1();
    TestCase2WithProcesses();
    TestCase3WithProcesses();
    return 0;
}
