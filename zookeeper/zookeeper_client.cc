#include "zookeeper_client.h"
#include <zookeeper/zookeeper.h>
#include <iostream>
#include <stdexcept>

// Callback for handling ZooKeeper connection events
void watcher(zhandle_t* zh, int type, int state, const char* path, void* watcherCtx) {
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            std::cout << "ZooKeeper client connected." << std::endl;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            std::cerr << "ZooKeeper session expired." << std::endl;
        }
    }
}

ZooKeeperClient::ZooKeeperClient(const std::string& zkServers) : zkServers(zkServers) {
    zkHandle = zookeeper_init(zkServers.c_str(), watcher, 30000, 0, nullptr, 0);
    if (!zkHandle) {
        throw std::runtime_error("Failed to connect to ZooKeeper.");
    }
    std::cout << "Connected to ZooKeeper: " << zkServers << std::endl;
}

ZooKeeperClient::~ZooKeeperClient() {
    if (zkHandle) {
        zookeeper_close(zkHandle);
        std::cout << "ZooKeeper connection closed." << std::endl;
    }
}

void ZooKeeperClient::ensurePathExists(const std::string& path) {
    if (zoo_exists(zkHandle, path.c_str(), 0, nullptr) == ZNONODE) {
        int rc = zoo_create(zkHandle, path.c_str(), nullptr, -1, &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
        if (rc != ZOK) {
            throw std::runtime_error("Failed to create path in ZooKeeper: " + path);
        }
    }
}

void ZooKeeperClient::createTopic(const std::string& topic, int numPartitions, int retentionMs, int replicationFactor) {
    std::string topicPath = "/topics/" + topic;
    ensurePathExists(topicPath);

    // Store topic metadata
    std::string metadata = "partitions:" + std::to_string(numPartitions) + ",retention:" + std::to_string(retentionMs) + ",replicas:" + std::to_string(replicationFactor);
    int rc = zoo_set(zkHandle, topicPath.c_str(), metadata.c_str(), metadata.size(), -1);
    if (rc != ZOK) {
        throw std::runtime_error("Failed to set metadata for topic: " + topic);
    }

    // Create partitions
    for (int i = 0; i < numPartitions; ++i) {
        std::string partitionPath = topicPath + "/" + std::to_string(i);
        ensurePathExists(partitionPath);
    }
    std::cout << "Topic created: " << topic << std::endl;
}

std::vector<std::string> ZooKeeperClient::getTopicPartitions(const std::string& topic) {
    std::string topicPath = "/topics/" + topic;
    struct String_vector partitions;

    int rc = zoo_get_children(zkHandle, topicPath.c_str(), 0, &partitions);
    if (rc != ZOK) {
        throw std::runtime_error("Failed to get partitions for topic: " + topic);
    }

    std::vector<std::string> partitionList;
    for (int i = 0; i < partitions.count; ++i) {
        partitionList.emplace_back(partitions.data[i]);
    }

    deallocate_String_vector(&partitions);
    return partitionList;
}

void ZooKeeperClient::assignPartitionToConsumer(const std::string& groupId, const std::string& topic, int partition, const std::string& consumerId) {
    std::string path = "/consumers/" + groupId + "/" + topic + "/" + std::to_string(partition) + "/consumer";
    ensurePathExists(path);
    int rc = zoo_set(zkHandle, path.c_str(), consumerId.c_str(), consumerId.size(), -1);
    if (rc != ZOK) {
        throw std::runtime_error("Failed to assign consumer to partition: " + path);
    }
    std::cout << "Partition " << partition << " assigned to consumer " << consumerId << std::endl;
}

std::string ZooKeeperClient::getConsumerForPartition(const std::string& groupId, const std::string& topic, int partition) {
    std::string path = "/consumers/" + groupId + "/" + topic + "/" + std::to_string(partition) + "/consumer";
    char buffer[256];
    int bufferLen = sizeof(buffer);

    int rc = zoo_get(zkHandle, path.c_str(), 0, buffer, &bufferLen, nullptr);
    if (rc != ZOK) {
        throw std::runtime_error("Failed to get consumer for partition: " + path);
    }
    return std::string(buffer, bufferLen);
}

void ZooKeeperClient::updateConsumerOffset(const std::string& groupId, const std::string& topic, int partition, int64_t offset) {
    std::string path = "/consumers/" + groupId + "/" + topic + "/" + std::to_string(partition) + "/offset";
    ensurePathExists(path);

    std::string offsetStr = std::to_string(offset);
    int rc = zoo_set(zkHandle, path.c_str(), offsetStr.c_str(), offsetStr.size(), -1);
    if (rc != ZOK) {
        throw std::runtime_error("Failed to update offset for partition: " + path);
    }
    std::cout << "Offset updated to " << offset << " for partition " << partition << std::endl;
}

int64_t ZooKeeperClient::getConsumerOffset(const std::string& groupId, const std::string& topic, int partition) {
    std::string path = "/consumers/" + groupId + "/" + topic + "/" + std::to_string(partition) + "/offset";
    char buffer[256];
    int bufferLen = sizeof(buffer);

    int rc = zoo_get(zkHandle, path.c_str(), 0, buffer, &bufferLen, nullptr);
    if (rc != ZOK) {
        throw std::runtime_error("Failed to get offset for partition: " + path);
    }
    return std::stoll(std::string(buffer, bufferLen));
}