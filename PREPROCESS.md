## Preprocess dataset
We assume you have Spark 2.x installed. 
Just run SPARK_HOME/bin/spark-shell and point your browser to http://localhost:4040 to monitor.

### Load file
```
val inputFile = "file:///Users/ytaras/Projects/other/kafka_join/perf_dump.txt"
// NOTE - This should be your path to input file
val input = spark.read.option("sep", "|").option("header", true).option("inferSchema", true).csv(inputFile)
input.printSchema // see what's inferred for data types
input.show // see data sample

val cardinalityMap = input.schema.toSeq.map { x => (x.name, input.select(x.name).distinct.count) }.toMap
// Now you can get yourself a coffee. That's going to take a while, but it will output number of unique values for each column