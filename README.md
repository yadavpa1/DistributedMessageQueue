# Distributed Message Queue

## Overview
This project implements a scalable and fault-tolerant messaging system designed to facilitate reliable communication between producers and consumers in distributed environments. The system implements message persistence, load balancing, and dynamic scaling, making it ideal for asynchronous messaging.

## Architecture
The architecture consists of multiple components working together:
- **Producers**: Send messages to the queue.
- **Consumers**: Retrieve and process messages.
- **Brokers**: Handle message routing between producers and consumers.
- **Apache ZooKeeper**: Manages metadata and leader election.
- **Apache BookKeeper**: Provides persistent and replicated message storage.

![Distributed Message Queue Architecture](assets/Architecture_DMQ_Server.png)

## Features
- **Durability**: Reliable, persistent message storage with data replication for guaranteed delivery.
- **Scalability**: Dynamic addition/removal of brokers and partitioning of topics to handle changing workloads.
- **Fault Tolerance**: Automatic failover with dynamic partition reassignment for uninterrupted message processing.
- **Performance**: High throughput and low latency for efficient message processing.
- **Data Delivery Semantics**: At-least-once delivery for reliable communication.

## Getting Started

### Prerequisites
- Java Development Kit (JDK)
- C++ Compiler
- CMake
- Apache ZooKeeper
- Apache BookKeeper
- gRPC

### Installation
1. **Clone the Repository**
   ```bash
   git clone https://github.com/yadavpa1/DistributedMessageQueue.git
   cd DistributedMessageQueue
   ```

2. **Build the Project**
   - For Java components:
     ```bash
     ./gradlew build
     ```
   - For C++ components:
     ```bash
     mkdir build && cd build
     cmake ..
     make
     ```

3. **Configure ZooKeeper**
   - Start ZooKeeper service:
     ```bash
     zkServer.sh start
     ```

4. **Run the System**
   ```bash
   ./run.sh
   ```

## Usage
- **Producing Messages**: Producers send messages to specific queues.
- **Consuming Messages**: Consumers subscribe to queues and process messages.
- **System Administration**: Monitor and manage brokers dynamically.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgments
Special thanks to the open-source community for providing tools and frameworks that made this project possible.
