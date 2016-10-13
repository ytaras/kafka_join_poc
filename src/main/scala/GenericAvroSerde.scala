import java.util

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serdes, Deserializer, Serializer, Serde}

/**
  * Created by ytaras on 10/13/16.
  */
object GenericAvroSerde {

  def generic() = Serdes.serdeFrom(
    new KafkaAvroSerializer(),
    new KafkaAvroDeserializer()
  ).asInstanceOf[Serde[GenericRecord]]

  def generic(client: SchemaRegistryClient) = Serdes.serdeFrom(
    new KafkaAvroSerializer(client),
    new KafkaAvroDeserializer(client)
  ).asInstanceOf[Serde[GenericRecord]]
}

