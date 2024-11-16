#ifndef MESSAGE_QUEUE_PRODUCER_H
#define MESSAGE_QUEUE_PRODUCER_H

#include <iostream>
#include <string>
#include <memory>

class MessageQueueProducer {
public:
    // Constructor that initializes the gRPC channel and stub using the server IP
    MessageQueueProducer(const std::string& server_ip);

    // Produces a message to the message queue
    bool ProduceMessage(const std::string& key, const std::string& value, const std::string& topic,
                        const std::string& producer_id, const std::string& ack_mode);

private:
    class Impl; // Forward declaration of the implementation class
    std::unique_ptr<Impl> impl_; // Pointer to the implementation class
};

#endif // MESSAGE_QUEUE_PRODUCER_H