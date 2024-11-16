#include "producer.h"
#include <iostream>
#include <string>

void RunProducerClient(const std::string& server_ip) {
    Producer producer(server_ip);

    std::string key, value, topic, producer_id, ack_mode;

    std::cout << "Enter Producer ID: ";
    std::getline(std::cin, producer_id);

    std::cout << "Enter Topic: ";
    std::getline(std::cin, topic);

    std::cout << "Enter Acknowledgment Mode (e.g., sync/async): ";
    std::getline(std::cin, ack_mode);

    while (true) {
        std::cout << "\nEnter a message (key value) or type 'exit' to quit:\n";
        std::cout << "Key: ";
        std::getline(std::cin, key);

        if (key == "exit") {
            break;
        }

        std::cout << "Value: ";
        std::getline(std::cin, value);

        if (value == "exit") {
            break;
        }

        bool success = producer.ProduceMessage(key, value, topic, producer_id, ack_mode);
        if (success) {
            std::cout << "Message sent successfully." << std::endl;
        } else {
            std::cout << "Failed to send message." << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <server_ip>" << std::endl;
        return 1;
    }

    const std::string server_ip = argv[1];
    RunProducerClient(server_ip);

    return 0;
}
