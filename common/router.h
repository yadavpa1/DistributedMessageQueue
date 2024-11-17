#ifndef MESSAGE_QUEUE_ROUTER_H
#define MESSAGE_QUEUE_ROUTER_H

#include <string>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "message_queue.grpc.pb.h"

class Router {
public:
    Router(const std::vector<std::string>& bootstrap_servers);

    // Gets the leader for a given topic and partition
    std::string GetLeader(const std::string& topic, int partition);

    // Gets the total number of partitions for a given topic
    int GetTotalPartitions(const std::string& topic);

    // Updates metadata for a given topic
    void UpdateMetadata(const std::string& topic);

    // Fetch routing table for a given topic periodically
    void StartPeriodicMetadataRefresh(int interval_ms); // Optional Feature. Call when router is initialized.

private:
    std::unordered_map<std::string, std::unordered_map<int, std::string>> routing_table_;
    std::unordered_map<std::string, int> topic_partitions_;
    std::mutex mutex_;
    std::unique_ptr<message_queue::message_queue::Stub> stub_;

    // Internal method to fetch metadata for a topic
    void FetchMetadata(const std::string& topic);
};

#endif // MESSAGE_QUEUE_ROUTER_H
