#ifndef MESSAGE_QUEUE_SYS_ADMIN_H
#define MESSAGE_QUEUE_SYS_ADMIN_H

#include <string>
#include <vector>
#include <memory>

class SysAdmin {
public:
    // Constructor to initialize producer with bootstrap servers
    SysAdmin(const std::vector<std::string>& bootstrap_servers);

    ~SysAdmin(); // Declare the destructor
    
    bool shutdown(const std::string &broker_id);

private:
    class Impl; // Forward declaration of the implementation class
    std::unique_ptr<Impl> impl_; // Pointer to the implementation class
};

#endif // MESSAGE_QUEUE_SYS_ADMIN_H