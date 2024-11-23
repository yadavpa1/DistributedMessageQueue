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
    std::string consumer_id;

public:
    Consumer(const std::vector<std::string> &bootstrap_servers, std::string consumer_id);
    ~Consumer();
    std::vector<MessageResponse> ConsumeMessage(std::string group_id, std::string topic, int partition, int offset, int max_messages);
    std::string get_consumer_id();
};

#endif // CONSUMER_H