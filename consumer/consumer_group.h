#ifndef CONSUMER_GROUP_H
#define CONSUMER_GROUP_H

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include "consumer.h"

struct topic_state {
    std::string topic;
    int partition;
    int offset;
};

class ConsumerGroup {
private:
    std::string tag;
    std::string group_id;
    std::vector<std::unique_ptr<Consumer>> consumers_;
    std::unordered_map<std::string, 
    std::unordered_map<std::string, std::vector<topic_state>>> consumer_topic_state_;
    std::unordered_map<std::string, std::string> topic_partition_consumer_;

    bool IsTopicConsumed(std::string topic, int partition);

public:
    ConsumerGroup(std::string tag, std::string group_id);
    ~ConsumerGroup();
    bool AddConsumer(std::string server_address, std::string consumer_id, std::vector<std::string> topics, std::vector<int> partitions, std::vector<int> offsets);
    bool RemoveConsumer(std::string consumer_id);
    std::vector<MessageResponse> ConsumeMessage(std::string topic, int partition, int max_messages);
    void PrintConsumerGroup();
};

#endif // CONSUMER_GROUP_H