#include "sys_admin.h"
#include <iostream>
#include <string>
#include <vector>

void RunSysAdminClient(const std::vector<std::string>& bootstrap_servers) {
    SysAdmin sys_admin(bootstrap_servers);
    
    while(true) {
        std::string broker_id;
        std::cout << "Enter Broker ID to shutdown or type 'exit' to quit: ";
        std::getline(std::cin, broker_id);

        if(broker_id == "exit") {
            break;
        }

        bool success = sys_admin.shutdown(broker_id);
        if(success) {
            std::cout << "Broker shutdown successfully." << std::endl;
        } else {
            std::cout << "Failed to shutdown broker." << std::endl;
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <bootstrap_server1> [<bootstrap_server2> ...]" << std::endl;
        return 1;
    }

    std::vector<std::string> bootstrap_servers;
    for (int i = 1; i < argc; ++i) {
        bootstrap_servers.push_back(argv[i]);
    }

    RunSysAdminClient(bootstrap_servers);

    return 0;
}
