#include "producer.h"
#include <iostream>
#include <string>
#include <vector>

void RunProducerClient(const std::vector<std::string>& bootstrap_servers) {
    std::string producer_id;
    std::cout << "Enter Producer ID: ";
    std::getline(std::cin, producer_id);

    int flush_threshold;
    std::cout << "Enter Flush Threshold: ";
    std::cin >> flush_threshold;

    int flush_interval_ms;
    std::cout << "Enter Flush Interval (ms): ";
    std::cin >> flush_interval_ms;

    Producer producer(bootstrap_servers, flush_threshold, flush_interval_ms, producer_id);

    std::string key, value, topic;


    std::cout << "Enter Topic: ";
    std::cin >> topic;

    while (true) {
        std::cout << "\nEnter a message (key value) or type 'exit' to quit:\n";
        std::cout << "Key: ";
        std::cin >> key;

        if (key == "exit") {
            break;
        }

        std::cout << "Value: ";
        std::cin >> value;

        if (value == "exit") {
            break;
        }

        bool success = producer.ProduceMessage(key, value, topic);
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
