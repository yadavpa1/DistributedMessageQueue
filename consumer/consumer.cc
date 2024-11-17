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
        stub_ = message_queue::message_queue::NewStub(channel);
    }

    std::vector<MessageResponse> ConsumeMessage(std::string topic) {
        message_queue::ConsumeMessageRequest request;
        request.set_topic(topic);

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
    std::unique_ptr<message_queue::message_queue::Stub> stub_;
};

Consumer::Consumer(std::string server_address) : impl_(std::make_unique<Impl>(server_address)) {}

Consumer::~Consumer() = default;

std::vector<MessageResponse> Consumer::ConsumeMessage(std::string topic) {
    return impl_->ConsumeMessage(topic);
}

/*

New consumer code that implements consumer groups:

#include "consumer.h"
#include <iostream>

Consumer::Consumer(const std::string& brokerAddress, const std::string& groupId, const std::string& consumerId)
    : groupId(groupId), consumerId(consumerId) {
    brokerStub = message_queue::message_queue::NewStub(grpc::CreateChannel(
        brokerAddress, grpc::InsecureChannelCredentials()));
}

void Consumer::consumeMessages(const std::string& topic, int partition, int maxMessages) {
    grpc::ClientContext context;
    message_queue::ConsumeMessageRequest request;
    message_queue::ConsumeMessageResponse response;

    request.set_group_id(groupId);
    request.set_topic(topic);
    request.set_partition(partition);
    request.set_max_messages(maxMessages);

    auto status = brokerStub->ConsumeMessage(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "Failed to consume messages: " << status.error_message() << std::endl;
        return;
    }

    if (!response.success()) {
        std::cerr << "ConsumeMessage error: " << response.error_message() << std::endl;
        return;
    }

    std::cout << "Consumed messages from topic: " << topic << ", partition: " << partition << std::endl;
    for (const auto& message : response.messages()) {
        std::cout << "Message Key: " << message.key() << ", Value: " << message.value() << std::endl;
        // Example: Process the message here
    }

    // Commit the last consumed offset
    if (!response.messages().empty()) {
        int64_t lastOffset = response.messages().rbegin()->offset();
        commitOffset(topic, partition, lastOffset);
    }
}

void Consumer::commitOffset(const std::string& topic, int partition, int64_t offset) {
    grpc::ClientContext context;
    message_queue::CommitOffsetRequest request;
    message_queue::CommitOffsetResponse response;

    request.set_group_id(groupId);
    request.set_topic(topic);
    request.set_partition(partition);
    request.set_offset(offset);

    auto status = brokerStub->CommitOffset(&context, request, &response);

    if (!status.ok()) {
        std::cerr << "Failed to commit offset: " << status.error_message() << std::endl;
        return;
    }

    if (!response.success()) {
        std::cerr << "CommitOffset error: " << response.error_message() << std::endl;
        return;
    }

    std::cout << "Offset committed: " << offset << " for topic: " << topic << ", partition: " << partition << std::endl;
}
*/