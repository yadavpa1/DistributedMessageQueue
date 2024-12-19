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


Producers:                              |-----------------------|                |-----------------------|
                                        |  THROUGHPUT (msg/sec) |                |     LATENCY (sec)     |
    Total Cycles: 10000                 |-----------------------|                |-----------------------|   
                                        | 10 Topics | 100Topics |                | 10 Topics | 100Topics |
                                        | 100 Batch | 10 Batch  |                | 100 Batch | 10 Batch  |
                                        | 3 Bookie  | 5 Bookies |                | 3 Bookie  | 5 Bookies |
    |-----------------------------------------------------------|                ------------------------|
    | No. of Brokers | No. of Producers | Throughput| Throughput|                |  Latency  |  Latency  |
    |----------------|------------------|-----------|-----------|                |-----------|-----------|
    | 3              | 1                |224        |108        |                |-          |0.00686    |
    | 3              | 2                |468        |143        |                |-          |0.00940    |
    | 3              | 4                |946        |205        |                |-          |0.0137     |
    | 3              | 8                |1780       |288        |                |-          |0.0206     |
    | 3              | 16               |2149       |302        |                |-          |0.0408     |
    | 3              | 32               | -         |239        |Partition-5     |-          |-          |  Partition- 5
    |-----------------------------------------------------------|-------------   |-----------------------|----------------
    | 5              | 1                |201        |113        |127             |-          |0.00692    |0.00768
    | 5              | 2                |469        |171        |                |-          |0.00872    |0.01113
    | 5              | 4                |922        |229        |                |-          |0.01331    |0.01706
    | 5              | 8                |1830       |287        |432             |-          |0.02182    |0.02792
    | 5              | 16               |2164       |294        |388             |-          |0.04780    |0.05348
    | 5              | 32               | -         |-          |                |-          |-          |
    |-----------------------------------------------------------|                |-----------------------|
    | 15             | 1                |208        |102        |                |-          |-          |
    | 15             | 2                |424        |149        |                |-          |-          |
    | 15             | 4                |850        |191        |                |-          |-          |
    | 15             | 8                |1538       |218        |                |-          |-          |
    | 15             | 16               |2058       |200        |                |-          |-          |
    | 15             | 32               | -         |-          |                |-          |-          |
    |------------------------------------------------------------                ------------------------|

    Batching:
           Total messages: 10000
           No. of Topics: 100
           No. of Partitions: 3
           No. of Brokers: 3
           No. of Bookie: 3
           No. of Producers: 1   
           Throughput measured in messages/sec
           Latency measured in sec                     
    |------------------------------------------|
    | Messages/Batch   | Throughput| Latency   |
    |------------------|-----------|-----------|
    | 1                |44         |-          |
    | 2                |57         |-          |
    | 4                |73         |-          |
    | 8                |98         |-          |
    | 16               |136        |-          |
    | 32               |209        |-          |
    | 64               |98         |-          |
    |------------------------------------------|

100Topics
10 Batch 
5 Bookies
Total messages: 10000
No. of Brokers: 3

 No. of Producers | Throughput
------------------|-----------
 1                |108        
 2                |143        
 4                |205        
 8                |288        
 16               |302        
 32               |239   

 No. of Consumers | Throughput
------------------|-----------
 1                |112        
 2                |136        
 4                |220        
 8                |285        
 16               |311        
 32               |245          
