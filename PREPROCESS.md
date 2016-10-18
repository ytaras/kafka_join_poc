## Preprocess dataset

Preprocessing is packaged as a Spark Job. 

### Prerequisites

1. Install Confluent platform v3.0.x from http://www.confluent.io
2. Start zookeeper, kafka broker/s and schema registry. Note - schema
 registry default port is 8081, but latest version of code looks at 
 You have either to change port in code or in schema registry properties
3. Create topics

```
    dim_part_8
    fact_part_8
    fact_part_8_key_by_join_key
    merged_output
``` 

4. Install sbt and run `sbt compile` or install IntelliJ and import project

### Running

1. Run `PrepareDataset` main class with single argument - name of directory 
which contains perf_dump.txt. It doesn't terminate itself, you have to
monitor progress at <http://localhost:4040>
2. Run `PublishData` with same argument.

Now your data is in Kafka topics


## Running KafkaStreams join

Just run `KafkaStreamsMap` and hit ENTER. It has some significant startup lag,
 but later you should see numbers running. 
 
Sample output:
```
M/ps: 31250.00 (20685.76 avg), with app start overhead - 19841.32, Total: 11991000
M/ps: 29411.76 (20686.28 avg), with app start overhead - 19841.85, Total: 11992000
M/ps: 31250.00 (20686.86 avg), with app start overhead - 19842.46, Total: 11993000
M/ps: 25000.00 (20687.16 avg), with app start overhead - 19842.80, Total: 11994000
M/ps: 29411.76 (20687.67 avg), with app start overhead - 19843.34, Total: 11995000
M/ps: 30303.03 (20688.22 avg), with app start overhead - 19843.91, Total: 11996000
M/ps: 23809.52 (20688.44 avg), with app start overhead - 19844.18, Total: 11997000
M/ps: 29411.76 (20688.95 avg), with app start overhead - 19844.72, Total: 11998000
M/ps: 28571.43 (20689.43 avg), with app start overhead - 19845.23, Total: 11999000
M/ps: 19607.84 (20689.33 avg), with app start overhead - 19845.21, Total: 12000000
M/ps: 26315.79 (20689.70 avg), with app start overhead - 19845.61, Total: 12001000
M/ps: 24390.24 (20689.96 avg), with app start overhead - 19845.92, Total: 12002000
```
 
