#include "producer.h"
#include <iostream>
#include <string>
#include <vector>

void RunProducerClient(const std::vector<std::string>& bootstrap_servers) {
    Producer producer(bootstrap_servers);

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
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <bootstrap_server1> [<bootstrap_server2> ...]" << std::endl;
        return 1;
    }

    std::vector<std::string> bootstrap_servers;
    for (int i = 1; i < argc; ++i) {
        bootstrap_servers.push_back(argv[i]);
    }

    RunProducerClient(bootstrap_servers);

    return 0;
}
