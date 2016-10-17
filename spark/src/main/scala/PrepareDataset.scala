import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._

/**
  * Created by ytaras on 10/17/16.
  */
object PrepareDataset extends App {
  val Array(pd) = args
  val parentDir = new File(pd)
  val sql = SparkSession.builder()
    .appName("prepare dataset")
    .master("local[*]")
    .getOrCreate()

  import sql.implicits._

  val input = sql.read
    .option("sep", "|").option("header", true)
    .option("inferSchema", true)
    .csv(new File(parentDir, "perf_dump.txt").getAbsolutePath)
    .cache


  val inputWithJoinKey = input.withColumn("join_key", substring($"_id", 0, 23))

  val facts = inputWithJoinKey
    .select($"_id", $"ip", $"m_label", $"m_max", $"m_val", from_unixtime($"m_time").as("m_time"), $"join_key")
  val dimension = inputWithJoinKey.select("join_key", "d_desc", "optx_hostname", "device")
    .dropDuplicates("join_key")
    .orderBy("device") // just to have different ordering than facts so we have more fair test
    .coalesce(16) // In previous stages we had 200 partitions which would result in 200 files on disc, which is not nice

  val outputDir = new File(parentDir, "avro_output").getAbsolutePath
  dimension.write.avro(s"$outputDir/dimension/")
  facts.write.avro(s"$outputDir/fact/")
  val canonical = facts.join(dimension, usingColumn = "join_key").orderBy("_id").cache
  canonical.write.option("header", true).csv(s"$outputDir/joinedCsv")
  canonical.write.json(s"$outputDir/joinedJson")
}
