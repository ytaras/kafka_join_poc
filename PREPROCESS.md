## Preprocess dataset
We assume you have Spark 2.x installed. 
Just run `SPARK_HOME/bin/spark-shell --driver-memory 4g` and point your browser to http://localhost:4040 to monitor.

### Load file
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
