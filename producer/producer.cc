#include "producer.h"
#include "router.h"
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <unordered_map>
#include <future>
#include <iostream>
#include <grpcpp/grpcpp.h>
#include "message_queue.grpc.pb.h"

int total_partitions = 3;

// Define the implementation class that was forward-declared in the header
class Producer::Impl {
public:
    Impl(const std::vector<std::string>& bootstrap_servers, int flush_threshold, int flush_interval_ms, const std::string& producer_id)
        : router_(std::make_unique<Router>(bootstrap_servers)),
          flush_threshold_(flush_threshold),
          flush_interval_ms_(flush_interval_ms),
          producer_id(producer_id) {}
    
    ~Impl() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            run_timers_ = false;
            for(auto &timer: broker_timers_) {
                timer.second.join();
            }
        }
    }

    bool ProduceMessage(const std::string& key, const std::string& value, const std::string& topic) {
        try {
            // Compute the target partition using key. total_partition is fixed to be 3.
            int partition = std::hash<std::string>{}(key) % total_partitions;

            // Get broker_ip for partition
            std::string broker_ip = router_->GetBrokerIP(topic, partition);

            std::cout << "Routing message to broker at: " << broker_ip << " for topic: " << topic
                      << ", partition: " << partition << std::endl;

            // Prepare the message
            message_queue::Message message;
            message.set_key(key);
            message.set_value(value);
            message.set_topic(topic);
            message.set_partition(partition);
            message.set_timestamp(time(nullptr));
            
            {
                std::lock_guard<std::mutex> lock(mutex_);
                message_map_[broker_ip].push_back(message);
                if (message_map_[broker_ip].size() >= flush_threshold_) {
                    FlushMessages(broker_ip);
                }

                // if(broker_timers_.find(broker_ip) == broker_timers_.end()) {
                //     broker_timers_[broker_ip] = std::thread(&Impl::FlushMessagesPeriodically, this, broker_ip);
                // }
            }
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Error in ProduceMessage: " << e.what() << std::endl;
        }

        return false;
    }

private:
    void FlushMessages(const std::string &broker_ip) {
        auto it = message_map_.find(broker_ip);
        if(it != message_map_.end()) {
            std::vector<message_queue::Message> messages = it->second;

            if(messages.empty()) {
                return;
            }
            
            auto channel = grpc::CreateChannel(broker_ip, grpc::InsecureChannelCredentials());
            auto stub = message_queue::MessageQueue::NewStub(channel);

            message_queue::ProduceMessagesRequest request;
            for(const auto &message: messages) {
                *request.add_messages() = message;
            }
            request.set_producer_id(producer_id);

            message_queue::ProduceMessagesResponse response;
            grpc::ClientContext context;
            grpc::Status status = stub->ProduceMessages(&context, request, &response);
            if(status.ok() && response.success()) {
                std::cout << "Successfully produced " << messages.size() << " messages to broker at: " << broker_ip << std::endl;
            } else {
                std::cerr << "Failed to produce messages to broker at: " << broker_ip << std::endl;
            }

            messages.clear();
        }
    }

    void FlushMessagesPeriodically(const std::string &broker_ip) {
        while(run_timers_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(flush_interval_ms_));
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if(!message_map_[broker_ip].empty()) {
                    FlushMessages(broker_ip);
                }
            }
        }
    }

    std::unique_ptr<Router> router_;
    std::unordered_map<std::string, std::vector<message_queue::Message>> message_map_;
    std::unordered_map<std::string, std::thread> broker_timers_;
    std::mutex mutex_;
    bool run_timers_ = true;
    int flush_threshold_;
    int flush_interval_ms_;
    std::string producer_id;
};

// Producer constructor
Producer::Producer(const std::vector<std::string>& bootstrap_servers, int flush_threshold, int flush_interval_ms, const std::string& producer_id)
    : impl_(std::make_unique<Impl>(bootstrap_servers, flush_threshold, flush_interval_ms, producer_id)) {}

Producer::~Producer() = default; // Defaulted destructor

bool Producer::ProduceMessage(const std::string& key, 
                              const std::string& value,
                              const std::string& topic) {
    return impl_->ProduceMessage(key, value, topic);
}