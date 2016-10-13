## Start kafka cluster
I recommend using confluent platform. Refer to http://docs.confluent.io/3.0.1/ 
and start ZK, Kafka and Schema Registry

Note - I've changed schema registry port on my machine to 18081, you may want to 
switch back to default one 8081


## Install sbt

http://www.scala-sbt.org/download.html

## Create topics
```
bin/kafka-topics --create --topic dimension_part_8  --partitions 8 --replication-factor 1 --zookeeper localhost:2181
bin/kafka-topics --create --topic facts_part_8  --partitions 8 --replication-factor 1 --zookeeper localhost:2181
```
Note - for a single machine you may want to use less partitions. Or not.

## Run importer

```
sbt "run <directory>"
```
First time it will take a while to download and compile everything.
