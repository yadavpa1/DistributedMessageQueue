#include "message_queue_server.h"
#include <chrono>
#include <iostream>

Partition::Partition(std::shared_ptr<bookkeeper::LedgerHandle> ledger)
    : ledgerHandle(ledger) {}

void Partition::appendMessage(const message_queue::Message& message) {
    std::lock_guard<std::mutex> lock(mutex);

    // Serialize the message
    std::string serializedMessage = message.SerializeAsString();

    // Write the message to BookKeeper ledger
    auto result = ledgerHandle->asyncAddEntry(serializedMessage);
    if (!result.is_ok()) {
        throw std::runtime_error("Failed to append message to ledger");
    }
}

std::vector<message_queue::Message> Partition::consumeMessages(int64_t start_offset, int max_messages) {
    std::lock_guard<std::mutex> lock(mutex);
    std::vector<message_queue::Message> result;

    // Read entries from BookKeeper ledger
    auto entries = ledgerHandle->readEntries(start_offset, start_offset + max_messages - 1);
    for (const auto& entry : entries) {
        message_queue::Message message;
        if (message.ParseFromString(entry.getData())) {
            result.push_back(message);
        }
    }

    return result;
}

MessageQueueServiceImpl::MessageQueueServiceImpl(const std::string& bookkeeperUri) {
    bookKeeperClient = std::make_shared<bookkeeper::BookKeeper>(bookkeeperUri);
}

grpc::Status MessageQueueServiceImpl::ProduceMessage(grpc::ServerContext* context,
                                                       const message_queue::ProduceMessageRequest* request,
                                                       message_queue::ProduceMessageResponse* response) {
    const auto& msg = request->message();
    const std::string& topic = msg.topic();
    int partition = msg.partition();

    std::lock_guard<std::mutex> lock(mutex);
    if (topicPartitions[topic].find(partition) == topicPartitions[topic].end()) {
        // Create a new ledger for the partition
        auto ledger = bookKeeperClient->createLedger();
        topicPartitions[topic][partition] = std::make_shared<Partition>(ledger);
    }

    try {
        topicPartitions[topic][partition]->appendMessage(msg);
        response->set_success(true);
        std::cout << "Message produced successfully." << std::endl;
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(e.what());
        std::cerr << "Failed to produce message: " << e.what() << std::endl;
    }

    return grpc::Status::OK;
}

grpc::Status MessageQueueServiceImpl::ConsumeMessage(grpc::ServerContext* context,
                                                       const message_queue::ConsumeMessageRequest* request,
                                                       message_queue::ConsumeMessageResponse* response) {
    const std::string& topic = request->topic();
    int partition = request->partition();
    int64_t startOffset = request->start_offset();
    int maxMessages = request->max_messages();

    std::lock_guard<std::mutex> lock(mutex);
    if (topicPartitions.find(topic) == topicPartitions.end() || 
        topicPartitions[topic].find(partition) == topicPartitions[topic].end()) {
            response->set_success(false);
            response->set_error_message("Topic or partition not found");
            return grpc::Status::OK;
    }

    try {
        auto messages = topicPartitions[topic][partition]->consumeMessages(startOffset, maxMessages);
        for (const auto& msg : messages) {
            auto* responseMessage = response->add_messages();
            *responseMessage = msg;
        }

        if (!it->status().ok()) {
            throw std::runtime_error(it->status().ToString());
        }

        std::cout << "Messages consumed successfully." << std::endl;
        response->set_success(true);

    } catch (const std::exception& ex) {
        response->set_success(false);
        response->set_error_message(e.what());
        std::cerr << "Failed to consume messages: " << e.what() << std::endl;
    }

    return grpc::Status::OK;
}