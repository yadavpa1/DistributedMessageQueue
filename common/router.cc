#include "router.h"
#include <iostream>

Router::Router(const std::vector<std::string>& bootstrap_servers) {
    // Iterate over bootstrap servers to find a reachable one
    for (const auto& server : bootstrap_servers) {
        try {
            auto channel = grpc::CreateChannel(server, grpc::InsecureChannelCredentials());
            stub_ = message_queue::message_queue::NewStub(channel);
            std::cout << "Connected to bootstrap server: " << server << std::endl;
            break;
        } catch (const std::exception& e) {
            std::cerr << "Failed to connect to bootstrap server: " << server << " - " << e.what() << std::endl;
        }
    }

    if (!stub_) {
        throw std::runtime_error("Failed to connect to any bootstrap server");
    }
}

std::string Router::GetLeader(const std::string& topic, int partition) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (routing_table_.count(topic) && routing_table_[topic].count(partition)) {
        return routing_table_[topic][partition];
    }
    throw std::runtime_error("Leader not found for topic: " + topic + ", partition: " + std::to_string(partition));
}

int Router::GetTotalPartitions(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (topic_partitions_.count(topic)) {
        return topic_partitions_[topic];
    }
    throw std::runtime_error("Partition count not available for topic: " + topic);
}

void Router::UpdateMetadata(const std::string& topic) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (routing_table_.count(topic) == 0) {
        std::cout << "Fetching metadata for topic: " << topic << std::endl;
        FetchMetadata(topic);
    } else {
        std::cout << "Metadata for topic: " << topic << " is already cached. Reading from cache" << std::endl;
    }
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
        topic_partitions_[topic] = response.partitions_size()
        for (const auto& partition : response.partitions()) {
            routing_table_[topic][partition.partition_id()] = partition.leader_address();
        }
    } else {
        std::cerr << "Failed to fetch metadata: " << (status.ok() ? response.error_message() : status.error_message()) << std::endl;
        throw std::runtime_error("Metadata fetch failed");
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
