#ifndef MESSAGE_QUEUE_SERVER_H
#define MESSAGE_QUEUE_SERVER_H

#include <unordered_map>
#include <vector>
#include <string>
#include <mutex>
#include <memory>
#include <bookkeeper/client.h>
#include "message_queue.grpc.pb.h"

class Partition {
    private:
        std::shared_ptr<bookkeeper::LedgerHandle> ledgerHandle;
        std::mutex mutex;

    public:
        Partition(std::shared_ptr<bookkeeper::LedgerHandle> ledger);
        void appendMessage(const message_queue::Message& message);
        std::vector<message_queue::Message> consumeMessages(int64_t start_offset, int max_messages);
};

class MessageQueueServiceImpl final : public message_queue::message_queue::Service {
    private:
        std::unordered_map<std::string, std::unordered_map<int, std::shared_ptr<Partition>>> topicPartitions;
        std::mutex mutex;
        std::shared_ptr<bookkeeper::BookKeeper> bookKeeperClient;

    public:
        MessageQueueServiceImpl(const std::string& bookkeeperUri);

        grpc::Status ProduceMessage(grpc::ServerContext* context,
                                 const message_queue::ProduceMessageRequest* request,
                                 message_queue::ProduceMessageResponse* response) override;
        
        grpc::Status ConsumeMessage(grpc::ServerContext* context,
                                 const message_queue::ConsumeMessageRequest* request,
                                 message_queue::ConsumeMessageResponse* response) override;
};

#endif // MESSAGE_QUEUE_SERVER_H