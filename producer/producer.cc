#include "producer.h"
#include "router.h"
#include <iostream>
#include <grpcpp/grpcpp.h>
#include "message_queue.grpc.pb.h"

// Define the implementation class that was forward-declared in the header
class Producer::Impl {
public:
    Impl(const std::vector<std::string>& bootstrap_servers)
        : router_(std::make_unique<Router>(bootstrap_servers)) {}

    bool ProduceMessage(const std::string& key, const std::string& value, const std::string& topic,
                        const std::string& producer_id, const std::string& ack_mode) {
        try {
            // Ensure metadata for the topic is available
            router_->UpdateMetadata(topic);

            // Fetch total partitions and compute the target partition
            int total_partitions = router_->GetTotalPartitions(topic);
            int partition = std::hash<std::string>{}(key) % total_partitions;

            // Get leader for partition
            std::string leader = router_->GetLeader(topic, partition);

            std::cout << "Routing message to leader: " << leader << " for topic: " << topic
                      << ", partition: " << partition << std::endl;

            // Create gRPC stub for the leader
            auto channel = grpc::CreateChannel(leader, grpc::InsecureChannelCredentials());
            auto stub = message_queue::message_queue::NewStub(channel);

            // Prepare the message
            message_queue::Message message;
            message.set_key(key);
            message.set_value(value);
            message.set_topic(topic);
            message.set_timestamp(time(nullptr)); // Current timestamp

            // Prepare the request
            message_queue::ProduceMessageRequest request;
            request.mutable_message()->CopyFrom(message);
            request.set_producer_id(producer_id);
            request.set_ack_mode(ack_mode);

            // Prepare the response
            message_queue::ProduceMessageResponse response;

            // Context for the client call
            grpc::ClientContext context;

            // Make the RPC call
            grpc::Status status = stub->ProduceMessage(&context, request, &response);

            // Check the result
            if (status.ok() && response.success()) {
                std::cout << "Message produced successfully." << std::endl;
                return true;
            } else {
                std::cerr << "Failed to produce message: " << (status.ok() ? response.error_message() : status.error_message()) << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Error in ProduceMessage: " << e.what() << std::endl;
        }

        return false;
    }

private:
    std::unique_ptr<Router> router_; // Router instance
};

// Producer constructor
Producer::Producer(const std::vector<std::string>& bootstrap_servers)
    : impl_(std::make_unique<Impl>(bootstrap_servers)) {}

Producer::~Producer() = default; // Defaulted destructor

bool Producer::ProduceMessage(const std::string& key, const std::string& value,
                                          const std::string& topic, const std::string& producer_id,
                                          const std::string& ack_mode) {
    return impl_->ProduceMessage(key, value, topic, producer_id, ack_mode);
}
