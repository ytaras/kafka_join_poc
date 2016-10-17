import java.util.Properties

import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer

/**
  * Created by ytaras on 10/13/16.
  */
object Producer {
 val props = new Properties()
 props.put("bootstrap.servers", "localhost:9092")
 props.put("acks", "all")
 props.put("schema.registry.url", "http://localhost:8081")

 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
 props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")

 val dimension = new KafkaProducer[String, GenericRecord](props)
 val fact = new KafkaProducer[Nothing, GenericRecord](props)
}
