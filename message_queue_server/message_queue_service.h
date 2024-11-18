#ifndef MESSAGE_QUEUE_SERVICE_H
#define MESSAGE_QUEUE_SERVICE_H

#include <grpcpp/grpcpp.h>
#include "message_queue.grpc.pb.h"
#include "rocksdb_wrapper.h"
#include <string>

class MessageQueueServiceImpl final : public message_queue::message_queue::Service {
    public:
    MessageQueueServiceImpl(const std::string& db_path);
    grpc::Status ProduceMessage(grpc::ServerContext* context,
                                const message_queue::ProduceMessageRequest* request,
                                message_queue::ProduceMessageResponse* response) override;

    grpc::Status ConsumeMessage(grpc::ServerContext* context,
                                const message_queue::ConsumeMessageRequest* request,
                                message_queue::ConsumeMessageResponse* response) override;
    
    private:
    RocksDBWrapper db_wrapper_;
};

#endif // MESSAGE_QUEUE_SERVICE_H
