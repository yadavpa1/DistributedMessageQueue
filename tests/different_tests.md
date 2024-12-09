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


Producers:                              |-----------------------------------||-----------------------------------|
                                        |       THROUGHPUT (msg/sec)        ||           LATENCY (sec)           |
    Total Cycles: 10000                 |-----------------------------------||-----------------------------------|   
                                        | 10 Topics | 10 Topics | 100Topics || 10 Topics | 10 Topics | 100Topics |
                                        | 100 Batch | 100 Batch | 10 Batch  || 100 Batch | 100 Batch | 10 Batch  |
                                        | 3 Bookie  | 15 Bookies| 5 Bookies || 3 Bookie  | 15 Bookies| 5 Bookies |
    |-----------------------------------------------------------------------|------------------------------------|
    | No. of Brokers | No. of Producers | Throughput| Throughput| Throughput||  Latency  |  Latency  |  Latency  |
    |----------------|------------------|-----------|-----------|-----------||-----------|-----------|-----------|
    | 3              | 1                |224        |203        |108        ||-          |-          |-          |
    | 3              | 2                |468        |469        |143        ||-          |-          |-          |
    | 3              | 4                |946        |921        |205        ||-          |-          |-          |
    | 3              | 8                |1780       |1789       |288        ||-          |-          |-          |
    | 3              | 16               |2149       |2112       |302        ||-          |-          |-          |
    | 3              | 32               | -         | -         |239        ||-          |-          |-          |
    |-----------------------------------------------------------------------||-----------------------------------|
    | 5              | 1                |201        |-          |113        ||-          |-          |-          |
    | 5              | 2                |469        |-          |171        ||-          |-          |-          |
    | 5              | 4                |922        |-          |229        ||-          |-          |-          |
    | 5              | 8                |1830       |-          |287        ||-          |-          |-          |
    | 5              | 16               |2164       |-          |294        ||-          |-          |-          |
    | 5              | 32               | -         |-          |-          ||-          |-          |-          |
    |-----------------------------------------------------------------------||-----------------------------------|
    | 15             | 1                |208        |-          |102        ||-         |-          |-           |
    | 15             | 2                |424        |-          |149        ||-         |-          |-           |
    | 15             | 4                |850        |-          |191        ||-         |-          |-           |
    | 15             | 8                |1538       |-          |218        ||-         |-          |-           |
    | 15             | 16               |2058       |-          |200        ||-         |-          |-           |
    | 15             | 32               | -         |-          |-          ||-         |-          |-           |
    |-----------------------------------------------------------------------|------------------------------------|


    