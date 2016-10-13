import java.io.File
import java.util.Properties

import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericRecord, GenericDatumReader}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.JavaConversions._

/**
  * Created by ytaras on 10/13/16.
  */
object ImportDimension extends App with AvroUtils {

  val dimensionAvro = "/Users/ytaras/Projects/other/kafka_join/avro_output/dimension"
  val stream = readAvroDir(dimensionAvro)
    .map(record => (record.get("join_key").toString, record))
    .map{ case (k, v) => new ProducerRecord("mock_dim", k, v) }
  sendToProducer(Producer.dimension, stream)
}

trait AvroUtils {

  def readAvroDir(str: String) =
    new File(str).listFiles.filter(_.getName.endsWith(".avro"))
    .toStream
    .flatMap(readAvro)
  def readAvro(f: File): Iterator[GenericRecord] = {
    DataFileReader
      .openReader(f, new GenericDatumReader[GenericRecord])
  }

  def sendToProducer[K, V](producer: KafkaProducer[K, V], s: Stream[ProducerRecord[K, V]]) = try {
    s.foreach(producer.send)
    producer.flush()
  } finally {
    producer.close()
  }
}


