#include "consumer.h"

Consumer::Consumer(std::string server_address) {
    std::shared_ptr<Channel> channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
    stub_ = MessageQueue::NewStub(channel);
}

ConsumeMessageResponse Consumer::consumeMessage(int start_offset, int max_messages) {
    ConsumeRequest request;
    request.set_start_offset(start_offset);
    request.set_max_messages(max_messages);

    ConsumeResponse response;
    ClientContext context;

    Status status = stub_->ConsumeMessage(&context, request, &response);

    if (status.ok()) {
        return response;
    } else {
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;
        return nullptr;
    }
}