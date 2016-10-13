import java.io.File
import java.util.Properties

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConversions._

/**
  * Created by ytaras on 10/13/16.
  */
object ImportDimension extends AvroUtils {
  def run(input: String) = {
    val topic = "dimension_part_8"
    println(s"Sending to topic $topic")
    val stream = readAvroDir(input)
      .map(record => (record.get("join_key").toString, record))
      .map{ case (k, v) => new ProducerRecord(topic, k, v) }
    sendToProducer(Producer.dimension, stream)
  }

}

trait AvroUtils {

  def readAvroDir(str: String) =
    new File(str).listFiles.filter(_.getName.endsWith(".avro"))
    .iterator
    .flatMap(readAvro)
  def readAvro(f: File): Iterator[GenericRecord] = {
    DataFileReader
      .openReader(f, new GenericDatumReader[GenericRecord])
  }

  // TODO - Use some parallelism. For now it's too slow
  def sendToProducer[K, V](producer: KafkaProducer[K, V], s: Iterator[ProducerRecord[K, V]]) = try {
    var acc: Long = 0
    s.grouped(1000).foreach { records =>
      records.foreach { r =>
        acc += 1
        producer.send(r)
      }
      producer.flush()
      println(s"Sent $acc records")
    }
  } finally {
    producer.close()
  }
}


