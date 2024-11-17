#ifndef MESSAGE_QUEUE_SERVER_H
#define MESSAGE_QUEUE_SERVER_H

#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include "message_queue.grpc.pb.h"
#include "zookeeper_client.h"
#include <bookkeeper/client.h>

class Partition {
private:
    std::shared_ptr<ZooKeeperClient> zkClient;
    std::shared_ptr<bookkeeper::LedgerHandle> ledgerHandle;
    std::string topic;
    int partition;
    std::string brokerId;

public:
    Partition(std::shared_ptr<ZooKeeperClient> zkClient, const std::string& topic, int partition, const std::string& brokerId);
    void appendMessage(const message_queue::Message& message);
    std::vector<message_queue::Message> fetchMessages(int64_t start_offset, int max_messages);
};

class MessageQueueServiceImpl final : public message_queue::message_queue::Service {
private:
    std::shared_ptr<ZooKeeperClient> zkClient;
    std::unordered_map<std::string, std::unordered_map<int, std::shared_ptr<Partition>>> topicPartitions;
    std::string brokerId;

public:
    MessageQueueServiceImpl(const std::string& zkServers, const std::string& brokerId);

    grpc::Status ProduceMessage(grpc::ServerContext* context,
                                 const message_queue::ProduceMessageRequest* request,
                                 message_queue::ProduceMessageResponse* response) override;

    grpc::Status ConsumeMessage(grpc::ServerContext* context,
                                 const message_queue::ConsumeMessageRequest* request,
                                 message_queue::ConsumeMessageResponse* response) override;

    grpc::Status CommitOffset(grpc::ServerContext* context,
                               const message_queue::CommitOffsetRequest* request,
                               message_queue::CommitOffsetResponse* response) override;
};

#endif // MESSAGE_QUEUE_SERVER_H