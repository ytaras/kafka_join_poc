import org.apache.kafka.clients.producer.ProducerRecord

/**
  * Created by ytaras on 10/13/16.
  */
object ImportFacts extends AvroUtils {
  def run(input: String) = {
    val topic = "facts"
    println(s"Sending to topic $topic")
    val stream = readAvroDir(input).map { record =>
      new ProducerRecord(topic, record)
    }
    sendToProducer(Producer.fact, stream)
  }

}
