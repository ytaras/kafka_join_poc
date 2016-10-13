import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by ytaras on 10/13/16.
  */
object ImportFacts extends App with AvroUtils {
  val input = "/Users/ytaras/Projects/other/kafka_join/avro_output/fact"
  val stream = readAvroDir(input).map { record =>
    new ProducerRecord("mock_fact", record)
  }.take(10)
   sendToProducer(Producer.fact, stream)

}
