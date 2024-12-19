#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <atomic>
#include <cstdlib>
#include <csignal>
#include <unistd.h>
#include <chrono>
#include <fstream>

#include <sys/wait.h>

#include "producer.h"
#include "consumer_group.h"

#include <map>
#include <stdexcept>

using namespace std;

/*                                       PERFORMANCE MEASURES
TEST CASE 1: Throughput and latency for producer
    TEST CASE 1.1: Throughput for producer (exclusive topic-partitions)
    TEST CASE 1.2: Latency for producer (exclusive topic-partitions)

TEST CASE 2: Throughput and latency for consumer_groups
    TEST CASE 2.1: Throughput for consumer_groups (exclusive topic-partitions)
    TEST CASE 2.2: Latency for consumer_groups (exclusive topic-partitions)

TEST CASE 3: Different batching strategies
    TEST CASE 3.1: Throughput for different batching strategies on producer
    TEST CASE 3.2: Latency for different batching strategies on producer

TEST CASE 4: Different batching strategies
    TEST CASE 4.1: Throughput for different batching strategies on consumer_group
    TEST CASE 4.2: Latency for different batching strategies on consumer_group

TOPICS
NORMAL READS
|consumer_group|consumer_gid|consumer_id|topic_list|partition_list|
|cg1|1|c1|{fin,fin,fin}|{0,1,2}|
|cg1|1|c2|{bus,bus,bus}|{0,1,2}|
*****************************************************************************************/

int TOTAL_CYCLES = 10000;
std::vector<std::string> BOOTSTRAP_SERVERS = {"localhost:8080", "localhost:8081", "localhost:8082"};
int FLUSH_THRESHOLD = 64;
int FLUSH_INTERVAL_MS = 10000000;
int MAX_MESSAGES = 1;
int NO_OF_TOPICS = 100;

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

std::vector<std::vector<std::string>> createTopics(int num_consumer_groups){
    std::vector<std::vector<std::string>> overall_topics;

    for(int i = 0;i<num_consumer_groups;++i){
        std::vector<std::string> topics;
        for(int j = 0;j<NO_OF_TOPICS;++j){
            std::string topic = GenerateRandomString(3);
            topics.push_back(topic);
        }
        overall_topics.push_back(topics);
    }

    return overall_topics;
}

void processOperationsWrite(Producer& producer, bool throughput_mode, std::vector<std::string> topics, std::atomic<bool> &failure_flag, double &metric){
    std::chrono::high_resolution_clock::time_point start_time = std::chrono::high_resolution_clock::now();
    for(int i = 0;i<TOTAL_CYCLES;++i){
        // Select random topic
        // std::cout << "Producing message " << i << std::endl;
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, topics.size()-1);
        std::string topic = topics[dis(gen)];
        std::string key = GenerateRandomString(3);
        std::string value = GenerateRandomString(3);
        if(!producer.ProduceMessage(key, value, topic)){
            failure_flag = true;
            return;
        }
    }
    std::chrono::high_resolution_clock::time_point end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    if(throughput_mode){
        metric = (double)TOTAL_CYCLES/elapsed_time.count();
    } else {
        metric = elapsed_time.count();
    }
    return;
}

void processOperationsRead(ConsumerGroup& consumerGroup, bool throughput_mode, std::vector<std::string> topics, std::atomic<bool> &failure_flag, double &metric){
    std::chrono::high_resolution_clock::time_point start_time = std::chrono::high_resolution_clock::now();
    int i = 0;
    for(i = 0;i<TOTAL_CYCLES/100;++i){
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, topics.size()-1);
        std::uniform_int_distribution<> dis_partition(0, 2);
        std::string topic = topics[dis(gen)];
        std::string partition_chosen = std::to_string(dis_partition(gen));
        std::vector<MessageResponse> messages = consumerGroup.ConsumeMessage(topic, std::stoi(partition_chosen), MAX_MESSAGES);
        if(messages.size()==0){
            break;
        }
    }
    std::chrono::high_resolution_clock::time_point end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    if(throughput_mode){
        metric = (double)(i+1)/elapsed_time.count();
    } else {
        metric = elapsed_time.count()/(double)(i+1);
    }
    return;
}

/* 
TESTS BEGIN HERE
*/

/*
############################# PRODUCER BENCHMARKING #############################
*/

void runBenchmarkWrite(int num_producers, bool throughput_mode){
    std::atomic<bool> failure_flag(false);

    int pipes[num_producers][2];
    for(int i = 0;i<num_producers;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(5, 30);
    std::vector<std::vector<std::string>> overall_topics = createTopics(NO_OF_TOPICS);  //dis(gen));

    // Fork child processes
    for (int i = 1; i <= num_producers; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double metric = 0.0;
            // Select a random set of topics
            std::random_device rd;
            std::mt19937 gen(rd());
            // std::uniform_int_distribution<> dis_top(0, overall_topics.size()-1);
            // std::vector<std::string> topics = overall_topics[dis_top(gen)];
            std::vector<std::string> topics = overall_topics[i-1];

            Producer producer(BOOTSTRAP_SERVERS, FLUSH_THRESHOLD, FLUSH_INTERVAL_MS, "pid"+std::to_string(i));
            processOperationsWrite(producer, throughput_mode, topics, failure_flag, metric);
            if(write(pipes[i-1][1], &metric, sizeof(metric))==-1){
                std::cerr << "FAILED TO WRITE METRIC FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_metric = 0.0;
    for(int i = 0;i<num_producers;i++){
        double metric = 0.0;
        if(read(pipes[i][0], &metric, sizeof(metric))==-1){
            std::cerr << "FAILED TO READ METRIC FROM PROCESS " << i+1 << endl;
        }
        total_metric += metric;
        close(pipes[i][0]);
    }

    // Wait for all child processes to finish
    for (int i = 0; i < num_producers; ++i) {
        int status;
        wait(&status);
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            failure_flag = true;
        }
    }

    if(failure_flag){
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    if(throughput_mode){
        cout << "THROUGHPUT FOR THE OPERATIONS: " << total_metric << endl;
    } else {
        cout << "LATENCY FOR THE OPERATIONS: " << (double)(total_metric/(double)num_producers)/TOTAL_CYCLES << endl;
    }
    return;
}

/*
############################# CONSUMER BENCHMARKING #############################
*/

void runBenchmarkRead(int num_consumers, bool throughput_mode){
    std::atomic<bool> failure_flag(false);

    int pipes[num_consumers][2];
    for(int i = 0;i<num_consumers;i++){
        if(pipe(pipes[i])==-1){
            std::cerr << "Pipe creation failed for process" << i+1 << endl;
        }
    }

    std::vector<std::vector<std::string>> overall_topics = createTopics(num_consumers);
    // fork and produce messages for all topics
    pid_t prod = fork();
    if(prod < 0){
        std::cerr << "Fork failed for producer" << std::endl;
        failure_flag = true;
    } else if(prod == 0){
        Producer producer(BOOTSTRAP_SERVERS, FLUSH_THRESHOLD, FLUSH_INTERVAL_MS, "pid");
        for(std::vector<std::string> topics: overall_topics){
            for(std::string topic: topics){
                for(int i = 0;i<TOTAL_CYCLES/100;i++){
                    std::string key = GenerateRandomString(3);
                    std::string value = GenerateRandomString(3);
                    if(!producer.ProduceMessage(key, value, topic)){
                        std::cout << "Failure here!" << std::endl;
                        failure_flag = true;
                        return;
                    }
                }
            }
        }
        exit(0);
    }

    int s1;
    wait(&s1);
    if (WIFEXITED(s1) && WEXITSTATUS(s1) != 0) {
        failure_flag = true;
    }

    std::cout << "Produced messages for all topics" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(10));

    // Fork child processes
    for (int i = 1; i <= num_consumers; ++i) {
        pid_t pid = fork();
        if (pid < 0) {
            std::cerr << "Fork failed for process " << i << std::endl;
            failure_flag = true;
            break;
        } else if (pid == 0) {
            close(pipes[i-1][0]);
            // Child process
            double metric = 0.0;
            // Select the current set of topics
            std::vector<std::string> topics = overall_topics[i-1];
            // Construct the consumer group
            ConsumerGroup consumerGroup("cg"+std::to_string(i), std::to_string(i));
            for(std::string topic: topics){
                consumerGroup.AddConsumer(BOOTSTRAP_SERVERS, "c"+topic, {topic, topic, topic}, {0, 1, 2}, {0, 0, 0});
            }
            consumerGroup.PrintConsumerGroup();
            processOperationsRead(consumerGroup, throughput_mode, topics, failure_flag, metric);
            if(write(pipes[i-1][1], &metric, sizeof(metric))==-1){
                std::cerr << "FAILED TO WRITE METRIC FROM PROCESS " << i << endl;
            }
            close(pipes[i-1][1]);
            exit(0);
        } else {
            close(pipes[i-1][1]);
        }
    }

    double total_metric = 0.0;
    for(int i = 0;i<num_consumers;i++){
        double metric = 0.0;
        if(read(pipes[i][0], &metric, sizeof(metric))==-1){
            std::cerr << "FAILED TO READ METRIC FROM PROCESS " << i+1 << endl;
        }
        total_metric += metric;
        close(pipes[i][0]);
    }

    // Wait for all child processes to finish
    for (int i = 0; i < num_consumers; ++i) {
        int status;
        wait(&status);
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
            failure_flag = true;
        }
    }

    if(failure_flag){
        cout << "FAILURE OCCURRED AT ONE OF THE CHILDREN" << endl;
        return;
    }

    if(throughput_mode){
        cout << "THROUGHPUT FOR THE OPERATIONS: " << total_metric << endl;
    } else {
        cout << "LATENCY FOR THE OPERATIONS: " << (double)(total_metric/(double)num_consumers) << endl;
    }
    return;
}

/*
TESTS END HERE
*/

std::map<std::string, std::string> parse_arguments(int argc, char* argv[]) {
    std::map<std::string, std::string> args;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "-p" || arg == "--processes") {
            if (i + 1 < argc) {
                args["processes"] = argv[++i];
            } else {
                throw std::invalid_argument("Missing value for argument: " + arg);
            }
        } else {
            throw std::invalid_argument("Unknown argument: " + arg);
        }
    }

    return args;
}

int main(int argc, char* argv[]){
    try {
        auto cl_options = parse_arguments(argc, argv);

        int num_processes = std::stoi(cl_options["processes"]);

        // TEST CASE 1.1: Throughput for producer (exclusive topic-partitions)
        runBenchmarkWrite(num_processes, true);

        // TEST CASE 1.2: Latency for producer (exclusive topic-partitions)
        // runBenchmarkWrite(num_processes, false);

        // TEST CASE 2.1: Throughput for consumer_groups (exclusive topic-partitions)
        // runBenchmarkRead(num_processes, true);

        // TEST CASE 2.2: Latency for consumer_groups (exclusive topic-partitions)
        // runBenchmarkRead(num_processes, false);

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}