#include "producer.h"
#include "router.h"
#include <iostream>
#include <grpcpp/grpcpp.h>
#include "message_queue.grpc.pb.h"

int total_partitions = 3;

// Define the implementation class that was forward-declared in the header
class Producer::Impl {
public:
    Impl(const std::vector<std::string>& bootstrap_servers)
        : router_(std::make_unique<Router>(bootstrap_servers)) {}

    bool ProduceMessage(const std::string& key, const std::string& value, const std::string& topic,
                        const std::string& producer_id, const std::string& ack_mode) {
        try {
            // Compute the target partition using key. total_partition is fixed to be 3.
            int partition = std::hash<std::string>{}(key) % total_partitions;

            // Get broker_ip for partition
            std::string broker_ip = router_->GetBrokerIP(topic, partition);

            std::cout << "Routing message to broker at: " << broker_ip << " for topic: " << topic
                      << ", partition: " << partition << std::endl;

            // Create gRPC stub for the broker_ip
            auto channel = grpc::CreateChannel(broker_ip, grpc::InsecureChannelCredentials());
            auto stub = message_queue::MessageQueue::NewStub(channel);

            // Prepare the message
            message_queue::Message message;
            message.set_key(key);
            message.set_value(value);
            message.set_topic(topic);
            message.set_partition(partition);
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
