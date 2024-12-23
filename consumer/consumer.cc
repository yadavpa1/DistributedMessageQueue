#include "consumer.h"
#include "router.h"

#include "message_queue.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <vector>
#include <string>

class Consumer::Impl {
public:
    Impl(const std::vector<std::string>& bootstrap_servers) {
        router_ = std::make_unique<Router>(bootstrap_servers);
    }

    std::vector<MessageResponse> ConsumeMessage(std::string group_id, std::string topic, int partition, int offset, int max_messages) {
        // Get broker ip for partition
        std::string broker_ip = router_->GetBrokerIP(topic, partition);

        std::cout << "Routing message to broker_ip: " << broker_ip << " for topic: " << topic
                  << ", partition: " << partition << std::endl;
        
        // Create gRPC stub for the broker_ip
        auto channel = grpc::CreateChannel(broker_ip, grpc::InsecureChannelCredentials());
        auto stub_ = message_queue::MessageQueue::NewStub(channel);

        message_queue::ConsumeMessagesRequest request;
        request.set_group_id(group_id);
        request.set_topic(topic);
        request.set_partition(partition);
        request.set_start_offset(offset);
        request.set_max_messages(max_messages);

        message_queue::ConsumeMessagesResponse response;
        grpc::ClientContext context;

        grpc::Status status = stub_->ConsumeMessages(&context, request, &response);

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
    std::unique_ptr<Router> router_;
};

Consumer::Consumer(const std::vector<std::string>& bootstrap_servers, std::string consumer_id) : impl_(std::make_unique<Impl>(bootstrap_servers)), consumer_id(consumer_id) {}

Consumer::~Consumer() = default;

std::vector<MessageResponse> Consumer::ConsumeMessage(std::string group_id, std::string topic, int partition, int offset, int max_messages) {
    return impl_->ConsumeMessage(group_id, topic, partition, offset, max_messages);
}

std::string Consumer::get_consumer_id() {
    return this->consumer_id;
}