#ifndef MESSAGE_QUEUE_PRODUCER_H
#define MESSAGE_QUEUE_PRODUCER_H

#include <string>
#include <vector>
#include <memory>

class Producer {
public:
    // Constructor to initialize producer with bootstrap servers
    Producer(const std::vector<std::string>& bootstrap_servers, int flush_threshold, int flush_interval_ms, const std::string& producer_id);

    ~Producer(); // Declare the destructor

    // Produces a message to the message queue
    bool ProduceMessage(const std::string& key, const std::string& value, const std::string& topic);

private:
    class Impl; // Forward declaration of the implementation class
    std::unique_ptr<Impl> impl_; // Pointer to the implementation class
};

#endif // MESSAGE_QUEUE_PRODUCER_H
