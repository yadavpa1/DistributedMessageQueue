#ifndef MESSAGE_QUEUE_ROUTER_H
#define MESSAGE_QUEUE_ROUTER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <grpcpp/grpcpp.h>
#include "message_queue.grpc.pb.h"

class Router {
public:
    Router(const std::vector<std::string>& bootstrap_servers);

    // Gets the broker for a given topic and partition
    std::string GetBrokerIP(const std::string& topic, int partition);

    // Gets the broker for a given broker id
    std::string GetBrokerIP(const std::string& broker_id);
    
     // Fetch routing table for a given topic periodically
    void StartPeriodicMetadataRefresh(int interval_ms); // Optional Feature. Call when router is initialized.

private:
    std::unordered_map<std::string, std::unordered_map<int, std::string>> routing_table_;
    std::unordered_map<std::string, int> topic_partitions_;
    std::mutex mutex_;
    std::unique_ptr<message_queue::MessageQueue::Stub> stub_;
    std::vector<std::string> bootstrap_servers_; // Store bootstrap servers
    
    // Internal method to fetch metadata for a topic
    void FetchMetadata(const std::string& topic);
    // Internal method to restablish stub for router if connection is lost
    bool ConnectToBootstrapServer();
};

#endif // MESSAGE_QUEUE_ROUTER_H
