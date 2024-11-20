#include "message_queue_service.h"
#include <sstream>

MessageQueueServiceImpl::MessageQueueServiceImpl(const std::string& db_path)
    : db_wrapper_(db_path) {}

grpc::Status MessageQueueServiceImpl::ProduceMessage(
    grpc::ServerContext* context, const message_queue::ProduceMessageRequest* request,
    message_queue::ProduceMessageResponse* response) {
        
    const auto& msg = request->message();
    std::ostringstream key;
    key << msg.topic() << ":" << msg.timestamp();

    std::ostringstream value;
    value << msg.key() << "|" << msg.value();

    try {
        db_wrapper_.put(key.str(), value.str());
        response->set_success(true);
        std::cout << "Message produced successfully." << std::endl;
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(e.what());
        std::cerr << "Failed to produce message: " << e.what() << std::endl;
    }
    return grpc::Status::OK;
}

grpc::Status MessageQueueServiceImpl::ConsumeMessage(
    grpc::ServerContext* context, const message_queue::ConsumeMessageRequest* request,
    message_queue::ConsumeMessageResponse* response) {
    try {
        std::string prefix = request->topic() + ":";
        std::unique_ptr<rocksdb::Iterator> it(db_wrapper_.createIterator());
        for (it->Seek(prefix); it->Valid() && it->key().ToString().find(prefix) == 0; it->Next()) {
            auto* msg = response->add_messages();
            std::string value = it->value().ToString();
            size_t delimiter_pos = value.find('|');
            msg->set_key(value.substr(0, delimiter_pos));
            msg->set_value(value.substr(delimiter_pos + 1));
            msg->set_topic(request->topic());
        }

        if (!it->status().ok()) {
            throw std::runtime_error(it->status().ToString());
        }

        std::cout << "Messages consumed successfully." << std::endl;
        response->set_success(true);
    } catch (const std::exception& e) {
        response->set_success(false);
        response->set_error_message(e.what());
        std::cerr << "Failed to consume messages: " << e.what() << std::endl;
    }
    return grpc::Status::OK;
}
