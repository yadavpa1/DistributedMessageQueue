#include "consumer.h"
#include <iostream>
#include <string>
#include <vector>

void RunConsumerClient(const std::string& server_address) {
    Consumer consumer(server_address);

    std::string topic;
    std::cout << "Enter Topic to Subscribe: ";
    std::getline(std::cin, topic);

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
        std::vector<MessageResponse> messages = consumer.ConsumeMessage(topic);

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
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <server_address>" << std::endl;
        return 1;
    }

    const std::string server_address = argv[1];
    RunConsumerClient(server_address);

    return 0;
}
