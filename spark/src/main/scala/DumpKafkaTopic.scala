import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by ytaras on 10/17/16.
  */
object DumpKafkaTopic extends App {
  val sql = SparkSession.builder()
    .appName("publish dataset")
    .master("local[*]")
    .config("spark.streaming.kafka.consumer.poll.ms", 2048)
    .getOrCreate()

  val ssc = new StreamingContext(sql.sparkContext, Seconds(5))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "schema.registry.url" -> "http://localhost:18081",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
    "group.id" -> "spark-dump-to-csv-1",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val dimStream = KafkaUtils.createDirectStream[String, GenericRecord](
    ssc, LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, GenericRecord](Seq("dim_part_8"), kafkaParams)
  )
  dimStream.foreachRDD { x => println(s"Dim size - ${x.count}") }
  dimStream.saveAsTextFiles("/tmp/dump-topics/dim")

  val factStream = KafkaUtils.createDirectStream[String, GenericRecord](
    ssc, LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, GenericRecord](Seq("fact_part_8"), kafkaParams)
  )
  factStream.foreachRDD { x => println(s"Fact size - ${x.count}") }
  factStream.saveAsTextFiles("/tmp/dump-topics/fact")

  ssc.start()
  ssc.awaitTermination()
}
