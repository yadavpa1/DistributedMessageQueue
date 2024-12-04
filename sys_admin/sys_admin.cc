#include "sys_admin.h"
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

class SysAdmin::Impl {
public:
    Impl(const std::vector<std::string>& bootstrap_servers)
        : router_(std::make_unique<Router>(bootstrap_servers)) {}

    ~Impl() = default;

    bool shutdown(const std::string &broker_id) {
        if(broker_info_.find(broker_id) != broker_info_.end()) {
            std::string broker_ip = broker_info_[broker_id];
            auto channel = grpc::CreateChannel(broker_ip, grpc::InsecureChannelCredentials());
            auto stub = message_queue::MessageQueue::NewStub(channel);

            message_queue::ShutdownRequest request;
            request.set_broker_id(broker_id);
            message_queue::ShutdownResponse response;

            grpc::ClientContext context;
            grpc::Status status = stub->Shutdown(&context, request, &response);
            if(status.ok() && response.success()) {
                std::cout << "Successfully shutdown broker with id: " << broker_id << std::endl;
                broker_info_.erase(broker_id);
                return true;
            } else if(status.ok()) {
                std::string new_broker_address = response.broker_address();
                
                if(new_broker_address.empty()) {
                    std::cerr << "Failed to shutdown broker with id: " << broker_id << std::endl;
                    return false;
                }

                auto new_channel = grpc::CreateChannel(new_broker_address, grpc::InsecureChannelCredentials());
                auto new_stub = message_queue::MessageQueue::NewStub(new_channel);
                message_queue::ShutdownRequest new_request;
                new_request.set_broker_id(broker_id);
                message_queue::ShutdownResponse new_response;

                grpc::ClientContext new_context;
                grpc::Status new_status = new_stub->Shutdown(&new_context, new_request, &new_response);

                if(new_status.ok() && new_response.success()) {
                    std::cout << "Successfully shutdown broker with id: " << broker_id << std::endl;
                    broker_info_.erase(broker_id);
                    return true;
                } else {
                    std::cerr << "Failed to shutdown broker with id: " << broker_id << std::endl;
                    return false;
                }
            } else {
                std::cerr << "Failed to shutdown broker with id: " << broker_id << std::endl;
                return false;
            }
        }

        std::string broker_ip = router_->GetBrokerIP(broker_id);
        if(broker_ip.empty()) {
            std::cerr << "Failed to get broker info for broker with id: " << broker_id << std::endl;
            return false;
        }
        
        auto channel = grpc::CreateChannel(broker_ip, grpc::InsecureChannelCredentials());
        auto stub = message_queue::MessageQueue::NewStub(channel);

        message_queue::ShutdownRequest request;
        request.set_broker_id(broker_id);
        message_queue::ShutdownResponse response;

        grpc::ClientContext context;
        grpc::Status status = stub->Shutdown(&context, request, &response);

        if(status.ok() && response.success()) {
            std::cout << "Successfully shutdown broker with id: " << broker_id << std::endl;
            return true;
        }
        std::cerr << "Failed to shutdown broker with id: " << broker_id << std::endl;
        return false;
    }

private:
    std::unique_ptr<Router> router_;
    std::unordered_map<std::string, std::string> broker_info_;
};

// Producer constructor
SysAdmin::SysAdmin(const std::vector<std::string>& bootstrap_servers)
    : impl_(std::make_unique<Impl>(bootstrap_servers)) {}

SysAdmin::~SysAdmin() = default; // Defaulted destructor

bool SysAdmin::shutdown(const std::string &broker_id) {
    return impl_->shutdown(broker_id);
}