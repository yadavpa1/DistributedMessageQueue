#ifndef CONSUMER_H
#define CONSUMER_H

#include <memory>
#include <string>
#include <vector>

struct MessageResponse {
    std::string key;
    std::string value;
    std::string topic;
    int timestamp;
};

class Consumer {
private:
    class Impl;
    std::unique_ptr<Impl> impl_;

public:
    Consumer(std::string server_address);
    ~Consumer();
    std::vector<MessageResponse> ConsumeMessage(std::string topic);
};

#endif // CONSUMER_H


/* New Consumer code:

#ifndef CONSUMER_H
#define CONSUMER_H

#include <string>
#include "message_queue.grpc.pb.h"
#include <grpcpp/grpcpp.h>

class Consumer {
private:
    std::string groupId;
    std::string consumerId;
    std::shared_ptr<message_queue::message_queue::Stub> brokerStub;

public:
    Consumer(const std::string& brokerAddress, const std::string& groupId, const std::string& consumerId);

    // Consume messages from a topic partition
    void consumeMessages(const std::string& topic, int partition, int maxMessages);

    // Commit the last consumed offset
    void commitOffset(const std::string& topic, int partition, int64_t offset);
};

#endif // CONSUMER_H

*/