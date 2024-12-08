# Tests Planned

TEST CASE 1: Throughput and latency for producer
    TEST CASE 1.1: Throughput for producer (exclusive topic-partitions)
    TEST CASE 1.2: Latency for producer (exclusive topic-partitions)

TEST CASE 2: Throughput and latency for consumer_groups
    TEST CASE 2.1: Throughput for consumer_groups (exclusive topic-partitions)
    TEST CASE 2.2: Latency for consumer_groups (exclusive topic-partitions)

TEST CASE 3: Different batching strategies
    TEST CASE 3.1: Throughput for different batching strategies on producer
    TEST CASE 3.2: Latency for different batching strategies on producer

TEST CASE 4: Different batching strategies
    TEST CASE 4.1: Throughput for different batching strategies on consumer_group
    TEST CASE 4.2: Latency for different batching strategies on consumer_group


Producers:
    BatchSize: 100
    TotalCycles: 10000
    | No. of Brokers | No. of Producers | Throughput|
    |----------------|------------------|-----------|
    | 3              | 1                | ?         |
    | 3              | 2                | ?         |
    | 3              | 4                | ?         |
    | 3              | 8                | ?         |
    | 3              | 16               | ?         |
    | 3              | 32               | ?         |
    | 3              | 64               | ?         |
   ----------------------------------------------------
    | 5              | 1                | ?         |
    | 5              | 2                | ?         |
    | 5              | 4                | ?         |
    | 5              | 8                | ?         |
    | 5              | 16               | ?         |
    | 5              | 32               | ?         |
   ----------------------------------------------------
    | 7              | 1                | ?         |
    | 7              | 2                | ?         |
    | 7              | 4                | ?         |
    | 7              | 8                | ?         |
    | 7              | 16               | ?         |
    | 7              | 32               | ?         |
   ----------------------------------------------------
    | 15             | 1                | ?         |
    | 15             | 2                | ?         |
    | 15             | 4                | ?         |
    | 15             | 8                | ?         |
    | 15             | 16               | ?         |
    | 15             | 32               | ?         |
    


    