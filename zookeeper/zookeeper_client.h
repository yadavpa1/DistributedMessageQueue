#ifndef ZOOKEEPER_CLIENT_H
#define ZOOKEEPER_CLIENT_H

#include <string>
#include <vector>

class ZooKeeperClient {
public:
    ZooKeeperClient(const std::string& zkServers);
    ~ZooKeeperClient();

    // Metadata management
    void createTopic(const std::string& topic, int numPartitions, int retentionMs, int replicationFactor);
    std::vector<std::string> getTopicPartitions(const std::string& topic);

    // State management
    void assignPartitionToConsumer(const std::string& groupId, const std::string& topic, int partition, const std::string& consumerId);
    std::string getConsumerForPartition(const std::string& groupId, const std::string& topic, int partition);

    void updateConsumerOffset(const std::string& groupId, const std::string& topic, int partition, int64_t offset);
    int64_t getConsumerOffset(const std::string& groupId, const std::string& topic, int partition);

private:
    zhandle_t* zkHandle;
    std::string zkServers;

    void ensurePathExists(const std::string& path);
};

#endif // ZOOKEEPER_CLIENT_H