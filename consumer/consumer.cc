#include "consumer.h"

#include "message_queue.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <vector>
#include <string>

class Consumer::Impl {
public:
    Impl(std::string server_address) {
        auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
        stub_ = message_queue::MessageQueue::NewStub(channel);
    }

    std::vector<MessageResponse> ConsumeMessage(std::string group_id, std::string topic, int partition, int offset, int max_messages) {
        message_queue::ConsumeMessageRequest request;
        request.set_group_id(group_id);
        request.set_topic(topic);
        request.set_partition(partition);
        request.set_start_offset(offset);
        request.set_max_messages(max_messages);

        message_queue::ConsumeMessageResponse response;
        grpc::ClientContext context;

        grpc::Status status = stub_->ConsumeMessage(&context, request, &response);

        if (!status.ok()) {
            std::cerr << "gRPC error: " << status.error_code() << ": " << status.error_message() << std::endl;
            return {};
        }

        if (!response.success()) {
            std::cerr << "ConsumeMessage failed: " << response.error_message() << std::endl;
            return {};
        }

        std::vector<MessageResponse> messages;
        for(const auto& message : response.messages()) {
            MessageResponse msg;
            msg.key = message.key();
            msg.value = message.value();
            msg.topic = message.topic();
            msg.timestamp = message.timestamp();
            messages.push_back(msg);
        }
        return messages;
    }

private:
    std::unique_ptr<message_queue::MessageQueue::Stub> stub_;
};

Consumer::Consumer(std::string server_address, std::string consumer_id) : impl_(std::make_unique<Impl>(server_address)), consumer_id(consumer_id) {}

Consumer::~Consumer() = default;

std::vector<MessageResponse> Consumer::ConsumeMessage(std::string group_id, std::string topic, int partition, int offset, int max_messages) {
    return impl_->ConsumeMessage(group_id, topic, partition, offset, max_messages);
}

std::string Consumer::get_consumer_id() {
    return this->consumer_id;
}