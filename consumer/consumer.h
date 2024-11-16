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