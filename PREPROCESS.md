## Preprocess dataset
We assume you have Spark 2.x installed. 
Just run `SPARK_HOME/bin/spark-shell --driver-memory 4g --packages com.databricks:spark-avro_2.11:3.0.1` and point your browser to http://localhost:4040 to monitor.

### Analyze file
```
val inputFile = "file:///Users/ytaras/Projects/other/kafka_join/perf_dump.txt"
// NOTE - This should be your path to input file
val input = spark.read
    .option("sep", "|").option("header", true)
    .option("inferSchema", true).csv(inputFile).cache
input.printSchema // see what's inferred for data types
input.show // see data sample

val cardinalityMap = input.schema.toSeq.map { x => (x.name, input.select(x.name).distinct.count) }.toMap
// Now you can get yourself a coffee. That's going to take a while, but it will output number of unique values for each column
```

I already calculated that for you, so you may skip last line:

|Metric|Unique values|
|------|-------------|
|m_desc|3|
|ip|501|
|optx_hostname|2|
|m_label|4|
|\_id|12002481|
|m_val|22|
|d_desc|507|
|m_max|3|
|metric|3|
|m_cat|3|
|m_unit|3|
|m_ds|3|
|m_time|357888|
|device|501|

We would like to try to join big facts dataset (12002481 input rows) with some 
dimension dataset, which have ~1M records. We don't have appropriate colunms
in our dataset, but we can easily get artificial join key by trimming 
_id column last character:

```
val inputWithJoinKey = input.withColumn("join_key", substring($"_id", 0, 23))
inputWithJoinKey.select($"join_key").distinct.count
```

Our artificial join key has 858086 which seems good for evaluation purpose.

### Split dataset

Now lets have some part of columns in facts dataset and some in dimension
dataset, where 'join_key' will be unique value for dimension:

```
val facts = inputWithJoinKey.select($"_id", $"ip", $"m_label", $"m_max", $"m_val", from_unixtime($"m_time").as("m_time"), $"join_key")
val dimension = inputWithJoinKey.select("join_key", "d_desc", "ip", "optx_hostname")
    .dropDuplicates("join_key")
    .orderBy("ip") // just to have different ordering than facts so we have more fair test
    .coalesce(16) // In previous stages we had 200 partitions which would result in 200 files on disc, which is not nice
```
You can have a look at datasets with `facts.show` and `dimension.show`


### Save to avro

Let's save this to avro files now so we can easily reuse them without need to calculate it again:
```
import com.databricks.spark.avro._
val outputDir = "file:///Users/ytaras/Projects/other/kafka_join/avro_output"
dimension.write.avro(s"$outputDir/dimension/")
facts.write.avro(s"$outputDir/fact/")
spark.read.avro(s"$outputDir/fact/").show
spark.read.avro(s"$outputDir/dimension/").show
```
Last two lines take sample of files stored