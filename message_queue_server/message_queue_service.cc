#include "message_queue_server.h"
#include <iostream>
#include <stdexcept>

Partition::Partition(std::shared_ptr<ZooKeeperClient> zkClient, const std::string& topic, int partition, const std::string& brokerId)
    : zkClient(zkClient), topic(topic), partition(partition), brokerId(brokerId) {
    // Initialize BookKeeper ledger for this partition
    auto client = bookkeeper::BookKeeper::createClient();
    ledgerHandle = client->createLedger();
    if (!ledgerHandle) {
        throw std::runtime_error("Failed to create ledger for partition.");
    }
    std::cout << "Ledger created for topic: " << topic << ", partition: " << partition << std::endl;
}

void Partition::appendMessage(const message_queue::Message& message) {
    std::string serializedMessage = message.SerializeAsString();
    auto result = ledgerHandle->asyncAddEntry(serializedMessage);
    if (!result.is_ok()) {
        throw std::runtime_error("Failed to append message to ledger.");
    }
    std::cout << "Message appended to ledger for topic: " << topic << ", partition: " << partition << std::endl;
}

std::vector<message_queue::Message> Partition::fetchMessages(int64_t start_offset, int max_messages) {
    std::vector<message_queue::Message> messages;
    auto entries = ledgerHandle->readEntries(start_offset, start_offset + max_messages - 1);
    for (const auto& entry : entries) {
        message_queue::Message message;
        if (message.ParseFromString(entry.getData())) {
            messages.push_back(message);
        }
    }
    return messages;
}

MessageQueueServiceImpl::MessageQueueServiceImpl(const std::string& zkServers, const std::string& brokerId)
    : zkClient(std::make_shared<ZooKeeperClient>(zkServers)), brokerId(brokerId) {
    zkClient->registerBroker(brokerId);
}

grpc::Status MessageQueueServiceImpl::ProduceMessage(grpc::ServerContext* context,
                                                       const message_queue::ProduceMessageRequest* request,
                                                       message_queue::ProduceMessageResponse* response) {
    const auto& msg = request->message();
    const std::string& topic = msg.topic();
    int partition = msg.partition();

    std::lock_guard<std::mutex> lock(mutex);
    if (topicPartitions[topic].find(partition) == topicPartitions[topic].end()) {
        topicPartitions[topic][partition] = std::make_shared<Partition>(zkClient, topic, partition, brokerId);
    }

    try {
        topicPartitions[topic][partition]->appendMessage(msg);
        response->set_success(true);
    } catch (const std::exception& ex) {
        response->set_success(false);
        response->set_error_message(ex.what());
    }

    return grpc::Status::OK;
}

grpc::Status MessageQueueServiceImpl::ConsumeMessage(grpc::ServerContext* context,
                                                       const message_queue::ConsumeMessageRequest* request,
                                                       message_queue::ConsumeMessageResponse* response) {
    const std::string& topic = request->topic();
    int partition = request->partition();

    std::lock_guard<std::mutex> lock(mutex);
    if (topicPartitions.find(topic) == topicPartitions.end() ||
        topicPartitions[topic].find(partition) == topicPartitions[topic].end()) {
        response->set_success(false);
        response->set_error_message("Partition not found");
        return grpc::Status::OK;
    }

    try {
        auto messages = topicPartitions[topic][partition]->fetchMessages(request->start_offset(), request->max_messages());
        for (const auto& msg : messages) {
            auto* responseMessage = response->add_messages();
            *responseMessage = msg;
        }
        response->set_success(true);
    } catch (const std::exception& ex) {
        response->set_success(false);
        response->set_error_message(ex.what());
    }

    return grpc::Status::OK;
}

grpc::Status MessageQueueServiceImpl::CommitOffset(grpc::ServerContext* context,
                                                    const message_queue::CommitOffsetRequest* request,
                                                    message_queue::CommitOffsetResponse* response) {
    const std::string& groupId = request->group_id();
    const std::string& topic = request->topic();
    int partition = request->partition();
    int64_t offset = request->offset();

    try {
        zkClient->updateConsumerOffset(groupId, topic, partition, offset);
        response->set_success(true);
    } catch (const std::exception& ex) {
        response->set_success(false);
        response->set_error_message(ex.what());
    }

    return grpc::Status::OK;
}