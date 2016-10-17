import java.util.concurrent.atomic.AtomicLong
import java.util.{Properties, UUID}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import scala.collection.JavaConversions._

/**
  * Created by ytaras on 10/13/16.
  */
object KafkaStreamsMap extends App {
  run(SampleJoinConfig)

  def run(config: JoinConfig) = {
    val appStartTime = System.currentTimeMillis()
    val builder: KStreamBuilder = new KStreamBuilder

    val streamingConfig = {
      val settings = new Properties

      // In real world this would be some stable API, but now I want to reprocess all data all time
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, s"app-id-random-${UUID.randomUUID().toString}")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
      settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      settings.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "6")
      settings.put("schema.registry.url", "http://localhost:18081")
      // Specify default (de)serializers for record keys and for record values.
      settings
    }

    // TODO - Figure out why I have to specify this by hand
    val client = new CachedSchemaRegistryClient("http://localhost:18081", 20)
    println(config.mergedSchema)
    val counter = new AtomicLong()
    val dimension: KTable[String, GenericRecord] = builder.table(Serdes.String(), GenericAvroSerde.generic(client), config.dimensionStream)
    val facts: KStream[String, GenericRecord] = builder
      .stream(Serdes.String(), GenericAvroSerde.generic(client), config.factStream)
      .map((_, v) => new KeyValue(v.get(config.joinKey).toString, v))
      .through(Serdes.String(), GenericAvroSerde.generic(client), s"${config.factStream}_key_by_${config.joinKey}")

    val joiner: ValueJoiner[GenericRecord, GenericRecord, GenericRecord] =
      (value1: GenericRecord, value2: GenericRecord) => config.mergeRecords(value1, value2)
    var startTime: Long = 0
    var prevTime: Long = 0
    facts.leftJoin(dimension, joiner)
      .mapValues { x =>
        val c = counter.incrementAndGet()
        if(c == 1) {
          prevTime = System.currentTimeMillis()
          startTime = System.currentTimeMillis()
        }
        if(c % 1000 == 0) {
          val current = System.currentTimeMillis()
          val messagesPsAvg = c.toDouble / (current - startTime) * 1000
          val messagesPsCur = 1000d / (current - prevTime) * 1000
          val messagesPsOverhead = c.toDouble / (current - appStartTime) * 1000
          prevTime = current
          println(s"Processed $c records")
          println(f"M/ps: $messagesPsCur%2.2f ($messagesPsAvg%2.2f avg), with app start overhead - $messagesPsOverhead%2.2f")
        }
        x
      }
      .to(Serdes.String(), GenericAvroSerde.generic(client), config.outputTo)
    val streams = new KafkaStreams(builder, streamingConfig)
    streams.start()
  }

}

object KeyValueImplicits {

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

}
