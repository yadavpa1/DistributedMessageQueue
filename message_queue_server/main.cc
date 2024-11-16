#include <grpcpp/grpcpp.h>
#include "message_queue_service.h"

void RunServer(const std::string& db_path) {
    std::string server_address("0.0.0.0:50051");
    MessageQueueServiceImpl service(db_path);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    server->Wait();
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <db_path>" << std::endl;
        return 1;
    }
    std::string db_path = argv[1];
    RunServer(db_path);
    return 0;
}
