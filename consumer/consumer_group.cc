#include "consumer_group.h"
#include <iostream>

ConsumerGroup::ConsumerGroup(std::string tag, std::string group_id) : tag(tag), group_id(group_id) {}

ConsumerGroup::~ConsumerGroup() = default;

bool ConsumerGroup::IsTopicConsumed(std::string topic, int partition) {
    // Check if the topic-partition is already in the set
    if(topic_partition_consumer_.find(topic + "-" + std::to_string(partition)) != topic_partition_consumer_.end()) {
        return true;
    }
    return false;
}

bool ConsumerGroup::AddConsumer(std::string server_address, std::string consumer_id, std::vector<std::string> topics, std::vector<int> partitions, std::vector<int> offsets) {
    // Check if the consumer is already present in the group
    for(const auto& consumer : consumers_) {
        if(consumer->get_consumer_id() == consumer_id) {
            std::cerr << "Consumer " << consumer_id << " is already present in the group" << std::endl;
            return false;
        }
    }
    // Check if the topics are already being consumed by another consumer
    for(const auto& topic : topics) {
        for(int i = 0; i < partitions.size(); i++) {
            if(IsTopicConsumed(topic, partitions[i])) {
                std::cerr << "Topic " << topic << " partition " << partitions[i] << " is already being consumed by another consumer" << std::endl;
                return false;
            }
        }
    }

    consumers_.push_back(std::make_unique<Consumer>(server_address, consumer_id));

    for(int i = 0; i < topics.size(); i++) {
        for(int j = 0; j < partitions.size(); j++) {
            topic_state state;
            state.topic = topics[i];
            state.partition = partitions[j];
            state.offset = offsets[j];
            consumer_topic_state_[consumer_id][topics[i]].push_back(state);
            topic_partition_consumer_.insert({topics[i] + "-" + std::to_string(partitions[j]), consumer_id});
        }
    }

    return true;
}

bool ConsumerGroup::RemoveConsumer(std::string consumer_id) {
    // Check if the consumer is present in the group
    bool found = false;
    for(int i = 0; i < consumers_.size(); i++) {
        if(consumers_[i]->get_consumer_id() == consumer_id) {
            found = true;
            consumers_.erase(consumers_.begin() + i);
            break;
        }
    }
    if(!found) {
        std::cerr << "Consumer " << consumer_id << " is not present in the group" << std::endl;
        return false;
    }
    // Remove the consumer from the topic state
    for(const auto& topic : consumer_topic_state_[consumer_id]) {
        for(const auto& state : topic.second) {
            topic_partition_consumer_.erase(topic.first + "-" + std::to_string(state.partition));
        }
    }
    consumer_topic_state_.erase(consumer_id);
    return true;
}

// Pull messages from the message queue
std::vector <MessageResponse> ConsumerGroup::ConsumeMessage(std::string topic, int partition, int max_messages) {
    std::vector <MessageResponse> messages;
    // Check if the topic-partition is being consumed by any consumer
    if(topic_partition_consumer_.find(topic + "-" + std::to_string(partition)) == topic_partition_consumer_.end()) {
        std::cerr << "Topic " << topic << " partition " << partition << " is not being consumed by any consumer" << std::endl;
        return messages;
    }

    std::string consumer_id = topic_partition_consumer_[topic + "-" + std::to_string(partition)];

    int offset = -1;
    for(const auto& state : consumer_topic_state_[consumer_id][topic]) {
        if(state.partition == partition) {
            offset = state.offset;
            break;
        }
    }


    if(offset == -1) {
        std::cerr << "Offset not found for topic " << topic << " partition " << partition << std::endl;
        return messages;
    }

    Consumer* consumer = nullptr;
    for(const auto& c : consumers_) {
        if(c->get_consumer_id() == consumer_id) {
            consumer = c.get();
            break;
        }
    }

    messages = consumer->ConsumeMessage(this->group_id, topic, partition, offset, max_messages);

    for(auto& state : consumer_topic_state_[consumer_id][topic]) {
        if(state.partition == partition) {
            state.offset += messages.size();
            break;
        }
    }

    return messages;
}

void ConsumerGroup::PrintConsumerGroup() {
    std::cout << "Consumer Group: " << tag << " - " << group_id << std::endl;
    for(const auto& consumer : consumers_) {
        std::cout << "Consumer ID: " << consumer->get_consumer_id() << std::endl;
        for(const auto& topic : consumer_topic_state_[consumer->get_consumer_id()]) {
            std::cout << "Topic: " << topic.first << std::endl;
            for(const auto& state : topic.second) {
                std::cout << "Partition: " << state.partition << ", Offset: " << state.offset << std::endl;
            }
        }
    }
}