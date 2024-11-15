#ifndef CONSUMER_H
#define CONSUMER_H

#include "message_queue.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using message_queue::MessageQueue;
using message_queue::Message;
using message_queue::ConsumeRequest;
using message_queue::ConsumeResponse;

// Create a consumer class
class Consumer {
    private:
        std::unique_ptr<MessageQueue::Stub> stub_;
    
    public:
        Consumer(std::string server_address);
        ConsumeMessageResponse consumeMessage(int start_offset, int max_messages);
}


#endif // CONSUMER_H