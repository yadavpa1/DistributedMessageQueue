#include "consumer_group.h"
#include <iostream>
#include <string>
#include <vector>

void RunConsumerClient(const std::vector<std::string>& bootstrap_servers) {
    std::string topic;
    std::cout << "Enter Topic to Subscribe: ";
    std::getline(std::cin, topic);

    ConsumerGroup consumer_group("test_group", "123");

    consumer_group.AddConsumer(bootstrap_servers, "consumer1", {topic}, {0}, {0});

    consumer_group.PrintConsumerGroup();

    std::cout << "Listening for messages on topic: " << topic << std::endl;

    while (true) {
        std::string user_input;
        std::cout << "Press Enter to fetch messages (or type 'exit' to quit): ";
        std::getline(std::cin, user_input);

        // Exit the loop if the user types 'exit'
        if (user_input == "exit") {
            std::cout << "Exiting the consumer client..." << std::endl;
            break;
        }

        // Fetch messages when the user presses Enter
        std::vector<MessageResponse> messages = consumer_group.ConsumeMessage(topic, 0, 10);

        if (!messages.empty()) {
            for (const auto& message : messages) {
                std::cout << "Received Message - Key: " << message.key
                          << ", Value: " << message.value
                          << ", Topic: " << message.topic
                          << ", Timestamp: " << message.timestamp << std::endl;
            }
        } else {
            std::cout << "No new messages." << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <bootstrap_server1> [<bootstrap_server2> ...]" << std::endl;
        return 1;
    }

    std::vector<std::string> bootstrap_servers;
    for (int i = 1; i < argc; ++i) {
        bootstrap_servers.push_back(argv[i]);
    }

    RunConsumerClient(bootstrap_servers);

    return 0;
}
