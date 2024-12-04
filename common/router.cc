#include "router.h"
#include <iostream>
#include <thread>
#include <algorithm> // For std::shuffle
#include <random>    // For std::random_device and std::mt19937

Router::Router(const std::vector<std::string>& bootstrap_servers)
    : bootstrap_servers_(bootstrap_servers) { // Initialize bootstrap servers
    // Iterate over bootstrap servers to find a reachable one
    if (!ConnectToBootstrapServer()) {
        throw std::runtime_error("Failed to connect to any bootstrap server");
    }
}

std::string Router::GetBrokerIP(const std::string& topic, int partition) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (routing_table_.count(topic) && routing_table_[topic].count(partition)) {
        return routing_table_[topic][partition];
    }

    // If partition leader is not found for a paritcular topic then fetch metadata for that topic
    std::cout << "Broker IP not found for topic: " << topic << ", partition: " << partition
              << ". Refreshing metadata..." << std::endl;

    FetchMetadata(topic);
    if (routing_table_.count(topic) && routing_table_[topic].count(partition)) {
        return routing_table_[topic][partition];
    }

    throw std::runtime_error("Failed to find leader after metadata refresh");
}

std::string Router::GetBrokerIP(const std::string& broker_id) {
    message_queue::BrokerAddressRequest request;
    request.set_broker_id(broker_id);

    message_queue::BrokerAddressResponse response;
    grpc::ClientContext context;
    grpc::Status status = stub_->GetBrokerAddress(&context, request, &response);

    if (status.ok() && response.success()) {
        return response.broker_address();
    }

    std::cerr << "Failed to get broker address for broker ID: " << broker_id << " - " << (status.ok() ? response.error_message() : status.error_message()) << std::endl;

    return "";
}

bool Router::ConnectToBootstrapServer() {
    std::cout << "Attempting to connect to a random bootstrap server..." << std::endl;

    // Create a copy of the bootstrap server list to shuffle
    std::vector<std::string> shuffled_servers = bootstrap_servers_;

    // Shuffle the list using a random number generator
    std::random_device rd; // Seed for randomness
    std::mt19937 g(rd());  // Mersenne Twister RNG
    std::shuffle(shuffled_servers.begin(), shuffled_servers.end(), g);

    // Attempt to connect to servers in the shuffled order
    for (const auto& server : shuffled_servers) {
        try {
            auto channel = grpc::CreateChannel(server, grpc::InsecureChannelCredentials());
            stub_ = message_queue::MessageQueue::NewStub(channel);
            std::cout << "Connected to bootstrap server: " << server << std::endl;
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to connect to bootstrap server: " << server << " - " << e.what() << std::endl;
        }
    }

    std::cerr << "All bootstrap servers are unavailable" << std::endl;
    return false;
}

void Router::FetchMetadata(const std::string& topic) {
    message_queue::MetadataRequest request;
    request.set_topic(topic);

    message_queue::MetadataResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetMetadata(&context, request, &response);

    if (status.ok() && response.success()) {
        std::cout << "Metadata fetched successfully for topic: " << topic << std::endl;
        routing_table_[topic].clear();
        topic_partitions_[topic] = response.partitions_size();
        for (const auto& partition : response.partitions()) {
            routing_table_[topic][partition.partition_id()] = partition.broker_address();
        }
    } else {
        std::cerr << "Failed to fetch metadata: " << (status.ok() ? response.error_message() : status.error_message()) << std::endl;

        // Attempt to reconnect to a different bootstrap server
        if (!ConnectToBootstrapServer()) {
            throw std::runtime_error("Metadata fetch failed and could not reconnect to a bootstrap server");
        }

        // Retry fetching metadata after reconnecting
        std::cerr << "Retrying metadata fetch for topic: " << topic << std::endl;
        FetchMetadata(topic);
    }
}


void Router::StartPeriodicMetadataRefresh(int interval_ms) {
    std::thread([this, interval_ms]() {
        while (true) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                for (const auto& entry : routing_table_) {
                    const std::string& topic = entry.first;
                    std::cout << "Periodically refreshing metadata for topic: " << topic << std::endl;
                    FetchMetadata(topic);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
        }
    }).detach(); // Run the thread in the background
}
