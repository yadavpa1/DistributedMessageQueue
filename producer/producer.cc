#include "producer.h"
#include <grpcpp/grpcpp.h>
#include "message_queue.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using message_queue::Message;
using message_queue::ProduceMessageRequest;
using message_queue::ProduceMessageResponse;
using message_queue::message_queue;

// Define the implementation class that was forward-declared in the header
class MessageQueueProducer::Impl {
public:
    Impl(const std::string& server_address) {
        channel_ = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        stub_ = message_queue::message_queue::NewStub(channel_);
        if (!stub_) {
            std::cerr << "Failed to create stub for server at " << server_address << std::endl;
            throw std::runtime_error("Stub initialization failed");
        }
    }

    bool ProduceMessage(const std::string& key, const std::string& value, const std::string& topic,
                        const std::string& producer_id, const std::string& ack_mode) {
        // Prepare the message
        Message message;
        message.set_key(key);
        message.set_value(value);
        message.set_topic(topic);
        message.set_timestamp(time(nullptr)); // Current timestamp

        // Prepare the request
        ProduceMessageRequest request;
        request.mutable_message()->CopyFrom(message);
        request.set_producer_id(producer_id);
        request.set_ack_mode(ack_mode);

        // Prepare the response
        ProduceMessageResponse response;

        // Context for the client call
        ClientContext context;

        // Make the RPC call
        Status status = stub_->ProduceMessage(&context, request, &response);

        // Check the result
        if (status.ok()) {
            if (response.success()) {
                std::cout << "Message produced successfully." << std::endl;
                return true;
            } else {
                std::cerr << "Failed to produce message: " << response.error_message() << std::endl;
            }
        } else {
            std::cerr << "RPC failed: " << status.error_message() << std::endl;
        }

        return false;
    }

private:
    std::shared_ptr<Channel> channel_;
    std::unique_ptr<message_queue::message_queue::Stub> stub_;
};

// MessageQueueProducer constructor
MessageQueueProducer::MessageQueueProducer(const std::string& server_address)
    : impl_(std::make_unique<Impl>(server_address)) {}

// MessageQueueProducer::ProduceMessage
bool MessageQueueProducer::ProduceMessage(const std::string& key, const std::string& value,
                                          const std::string& topic, const std::string& producer_id,
                                          const std::string& ack_mode) {
    return impl_->ProduceMessage(key, value, topic, producer_id, ack_mode);
}
