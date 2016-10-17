import java.io.File
import java.util.Properties

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by ytaras on 10/17/16.
  */
object PublishData extends App {
  val Array(pd) = args
  val parentDir = new File(pd, "avro_output")

  val sql = SparkSession.builder()
    .appName("publish dataset")
    .master("local[*]")
    .getOrCreate()
  val sc = sql.sparkContext
  println(sc.getConf.getLong("spark.streaming.kafka.consumer.poll.ms", 0L))

  val dimension = loadAvroRDD(sc, new File(parentDir, "dimension").getAbsolutePath)
  publishRDDToKafka("dim_part_8", dimension, Some("join_key"))

  val facts = loadAvroRDD(sc, new File(parentDir, "fact").getAbsolutePath)
  publishRDDToKafka("fact_part_8", dimension)


  def loadAvroRDD(sc: SparkContext, path: String): RDD[GenericRecord] = {
    sc.hadoopFile(path,
      classOf[AvroInputFormat[GenericRecord]],
      classOf[AvroWrapper[GenericRecord]],
      classOf[NullWritable]
    )
  }.keys.map(_.datum)

  def publishRDDToKafka(topic: String, rdd: RDD[GenericRecord], key: Option[String] = None) = {
    rdd
      .map(toProducerRecord(topic, _, key))
    .foreachPartition { partition =>
      val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("acks", "all")
      props.put("schema.registry.url", "http://localhost:18081")

      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
      val producer = new KafkaProducer[String, GenericRecord](props)
      partition.foreach(x => producer.send(x))
      producer.flush()
      producer.close()
    }
  }

  def toProducerRecord(topic: String, record: GenericRecord, key: Option[String]) = {
    if(key.isDefined)
      new ProducerRecord[String, GenericRecord](topic, record.get(key.get).toString, record)
    else
      new ProducerRecord[String, GenericRecord](topic, record)
  }
}
