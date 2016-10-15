import java.util.concurrent.atomic.AtomicLong
import java.util.{Properties, UUID}

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.monitoring.clients.interceptor.{MonitoringConsumerInterceptor, MonitoringProducerInterceptor}
import org.apache.avro.{Schema, SchemaBuilder}
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
  run

  def run = {
    val builder: KStreamBuilder = new KStreamBuilder

    val streamingConfig = {
      val settings = new Properties

      // In real world this would be some stable API, but now I want to reprocess all data all time
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, s"app-id-random-${UUID.randomUUID().toString}")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181")
      settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      settings.put("schema.registry.url", "http://localhost:8081")
      // Specify default (de)serializers for record keys and for record values.
      settings
    }

    val max = 10;
    val counter = new AtomicLong

    // TODO - Figure out why I have to specify this by hand
    val client = new CachedSchemaRegistryClient("http://localhost:8081", 20)
    val mergedSchema = {
      val factSchema = Schema.parse(client.getLatestSchemaMetadata("dimension_part_8-value").getSchema)
      val dimSchema = Schema.parse(client.getLatestSchemaMetadata("facts_part_8-value").getSchema)
      val fields = (factSchema.getFields ++ dimSchema.getFields).groupBy(_.name())
        .map { case (_, v) => v.head }.zipWithIndex.map { case (i, f) =>
        new Schema.Field(i.name(), i.schema, i.doc(), i.defaultVal())
      }
      Schema.createRecord("top", null, null, false, fields.toList)
    }
    println(mergedSchema)
    val dimension: KTable[String, GenericRecord] = builder.table(Serdes.String(), GenericAvroSerde.generic(client), "dimension_part_8")
    val facts: KStream[String, GenericRecord] = builder
      .stream(Serdes.String(), GenericAvroSerde.generic(client), "facts_part_8")
      .map((_, v) => new KeyValue(v.get("join_key").toString, v))
      .through(Serdes.String(), GenericAvroSerde.generic(client), "facts_keyed_by_join_key_part_8")

    val joiner: ValueJoiner[GenericRecord, GenericRecord, GenericRecord] = { (v1, v2) =>
      // TODO - Impelement AVRO serialization
      //
      val res = new Record(mergedSchema)
       v1.getSchema.getFields.forEach {
         f => res.put(f.name(), v1.get(f.name()))
       }
      if(v2 != null) {
        v2
          .getSchema
          .getFields
          .forEach {
            f => res.put(f.name(), v2.get(f.name()))
          }
      }
      res
    }
    //val joined: KStream[String, GenericRecord] =
    facts.leftJoin(dimension, joiner)
      .to(Serdes.String(), GenericAvroSerde.generic(client), "merged_facts_and_dimensions")
    val streams = new KafkaStreams(builder, streamingConfig)
    facts.foreach { (_, _) =>
      if(counter.incrementAndGet() > max) {
        streams.close()
      }
    }

    streams.start()
  }

}

object KeyValueImplicits {

  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] = new KeyValue(tuple._1, tuple._2)

}
