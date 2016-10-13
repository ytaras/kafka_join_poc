import java.util.{UUID, Properties}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.{KeyValue, KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import collection.JavaConversions._

/**
  * Created by ytaras on 10/13/16.
  */
object KafkaStreamsMap extends App {
  run
  import KeyValueImplicits._

  def run = {
    val builder: KStreamBuilder = new KStreamBuilder

    val streamingConfig = {
      val settings = new Properties

      // In real world this would be some stable API, but now I want to reprocess all data all time
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, s"app-id-random-${UUID.randomUUID().toString}")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
      settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      settings.put("schema.registry.url", "http://localhost:18081")
      // Specify default (de)serializers for record keys and for record values.
      settings
    }


    // TODO - Figure out why I have to specify this by hand
    val client = new CachedSchemaRegistryClient("http://localhost:18081", 20)
    val mergedSchema = {
      val factSchema = Schema.parse(client.getLatestSchemaMetadata("dimension_part_8-value").getSchema)
      println(factSchema.getName)
      val dimSchema = Schema.parse(client.getLatestSchemaMetadata("facts_part_8-value").getSchema)

      val fsRenamed = Schema.createRecord("fact", factSchema.getDoc, factSchema.getNamespace, factSchema.isError, factSchema.getFields)
      val dsRenamed = Schema.createRecord("dim", dimSchema.getDoc, dimSchema.getNamespace, factSchema.isError, factSchema.getFields)
      SchemaBuilder.record("tuple")
        .fields()
        .name("fact").`type`(fsRenamed).noDefault()
        .name("dim").`type`(dsRenamed).noDefault()
        .endRecord
    }
    println(mergedSchema)
    val dimension: KTable[String, GenericRecord] = builder.table(Serdes.String(), GenericAvroSerde.generic(client), "dimension_part_8")
    val facts: KStream[String, GenericRecord] = builder
      .stream(Serdes.String(), GenericAvroSerde.generic(client), "facts_part_8")
      .map((_, v) => new KeyValue(v.get("join_key").toString, v))
      .through(Serdes.String(), GenericAvroSerde.generic(client), "facts_keyed_by_join_key_part_8")

    val joiner: ValueJoiner[GenericRecord, GenericRecord, String] = { (v1, v2) =>
      // TODO - Impelement AVRO serialization
      //
      s"""{ fact: $v1, dim: $v2 }"""
    }
    //val joined: KStream[String, GenericRecord] =
    facts.leftJoin(dimension, joiner)
      .to(Serdes.String(), Serdes.String(), "merged_facts_and_dimensions")

    new KafkaStreams(builder, streamingConfig).start()
  }

}

object KeyValueImplicits {

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

}
