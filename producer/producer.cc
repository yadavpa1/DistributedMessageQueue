#include "producer.h"
#include <iostream>
#include <grpcpp/grpcpp.h>
#include "message_queue.grpc.pb.h"

// Define the implementation class that was forward-declared in the header
class Producer::Impl {
public:
    Impl(const std::string& server_address) {
        auto channel_ = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        stub_ = message_queue::message_queue::NewStub(channel_);
        if (!stub_) {
            std::cerr << "Failed to create stub for server at " << server_address << std::endl;
            throw std::runtime_error("Stub initialization failed");
        }
    }

    bool ProduceMessage(const std::string& key, const std::string& value, const std::string& topic,
                        const std::string& producer_id, const std::string& ack_mode) {
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
        grpc::Status status = stub_->ProduceMessage(&context, request, &response);

        // Check the result
        if (status.ok()) {
            if (response.success()) {
                std::cout << "Message produced successfully." << std::endl;
                return true;
            } else {
                std::cerr << "Failed to produce message: " << response.error_message() << std::endl;
            }
        } else {
            std::cerr << status.error_code() << "RPC failed: " << status.error_message() << std::endl;
        }

        return false;
    }

private:
    std::unique_ptr<message_queue::message_queue::Stub> stub_;
};

// Producer constructor
Producer::Producer(const std::string& server_address)
    : impl_(std::make_unique<Impl>(server_address)) {}

bool Producer::ProduceMessage(const std::string& key, const std::string& value,
                                          const std::string& topic, const std::string& producer_id,
                                          const std::string& ack_mode) {
    return impl_->ProduceMessage(key, value, topic, producer_id, ack_mode);
}
