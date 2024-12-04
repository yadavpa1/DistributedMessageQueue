#include "consumer_group.h"
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <memory>

const std::vector<std::string> BOOTSTRAP_SERVERS = {"localhost:8080", "localhost:8081", "localhost:8082"};

std::unordered_map<std::string, std::unique_ptr<ConsumerGroup>> readConsumerGroupConfig(const std::string& cg_config) {
    std::unordered_map<std::string, std::unique_ptr<ConsumerGroup>> consumer_groups;
    std::unordered_map<std::string, std::vector<std::vector<std::string>>> cg_consumer_topic_partition;
    std::ifstream file(cg_config);
    if (!file.is_open()) {
        std::cerr << "Failed to open consumer group config file: " << cg_config << std::endl;
        return consumer_groups;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string cg_tag, cg_gid, consumer_id, topic;
        int partition;
        if (!(iss >> cg_tag >> cg_gid >> consumer_id >> topic >> partition)) {
            std::cerr << "Failed to parse consumer group config line: " << line << std::endl;
            continue;
        }

        // Ensure consumer group exists
        if (consumer_groups.find(cg_tag) == consumer_groups.end()) {
            consumer_groups[cg_tag] = std::make_unique<ConsumerGroup>(cg_tag, cg_gid);
        }

        // Map topics and partitions for the consumer group
        cg_consumer_topic_partition[cg_tag].push_back({consumer_id, topic, std::to_string(partition)});
    }

    // For each consumer group, add consumers
    for (const auto& cg : cg_consumer_topic_partition) {
        if (consumer_groups.find(cg.first) == consumer_groups.end()) {
            std::cerr << "Consumer group tag missing: " << cg.first << std::endl;
            continue;
        }

        std::unordered_map<std::string, std::vector<std::vector<std::string>>> consumer_topic_partition;
        for (const auto& ctp : cg.second) {
            consumer_topic_partition[ctp[0]].push_back({ctp[1], ctp[2]});
        }

        for (const auto& ctp : consumer_topic_partition) {
            std::vector<std::string> topics;
            std::vector<int> partitions;
            std::vector<int> offsets;

            for (const auto& tp : ctp.second) {
                topics.push_back(tp[0]);

                try {
                    partitions.push_back(std::stoi(tp[1]));
                } catch (const std::exception& e) {
                    std::cerr << "Invalid partition value: " << tp[1] << std::endl;
                    continue;
                }

                offsets.push_back(0);
            }

            consumer_groups[cg.first]->AddConsumer(BOOTSTRAP_SERVERS, ctp.first, topics, partitions, offsets);
        }
    }

    return consumer_groups;
}

void RunConsumerClient() {
    auto consumer_groups = readConsumerGroupConfig("cg_config");

    if (consumer_groups.empty()) {
        std::cerr << "Failed to read consumer group config" << std::endl;
        return;
    }

    // Print the consumer group details
    for (const auto& cg : consumer_groups) {
        const auto& consumer_group = cg.second;
        consumer_group->PrintConsumerGroup();
    }

    while (true) {
        // Ask user to input consumer group tag
        std::string cg_tag;
        std::cout << "Enter consumer group tag: ";
        std::cin >> cg_tag;

        // Check if the consumer group tag is valid
        if (consumer_groups.find(cg_tag) == consumer_groups.end()) {
            std::cerr << "Invalid consumer group tag: " << cg_tag << std::endl;
            continue;
        }

        // Ask user to input topic and partition
        std::string topic;
        int partition;
        std::cout << "Enter topic: ";
        std::cin >> topic;
        std::cout << "Enter partition: ";
        std::cin >> partition;

        // Ask user to input max messages
        int max_messages;
        std::cout << "Enter max messages: ";
        std::cin >> max_messages;

        // Consume messages from the consumer group
        std::vector<MessageResponse> messages = consumer_groups[cg_tag]->ConsumeMessage(topic, partition, max_messages);

        // Print the messages
        if (!messages.empty()) {
            std::cout << "Consumed " << messages.size() << " messages:" << std::endl;
            for (const auto& message : messages) {
                std::cout << "Key: " << message.key << ", Value: " << message.value << ", Topic: " << message.topic << ", Timestamp: " << message.timestamp << std::endl;
            }
        } else {
            std::cout << "No new messages." << std::endl;
        }

        // Ask user if they want to continue
        std::string choice;
        std::cout << "Do you want to continue? (yes/no): ";
        std::cin >> choice;
        if (choice != "yes") {
            break;
        }
    }
}

int main(int argc, char* argv[]) {
    // if (argc < 2) {
    //     std::cerr << "Usage: " << argv[0] << " <bootstrap_server1> [<bootstrap_server2> ...]" << std::endl;
    //     return 1;
    // }

    // std::vector<std::string> bootstrap_servers;
    // for (int i = 1; i < argc; ++i) {
    //     bootstrap_servers.push_back(argv[i]);
    // }

    RunConsumerClient();

    return 0;
}
