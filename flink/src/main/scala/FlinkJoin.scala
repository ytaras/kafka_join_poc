import java.io.ByteArrayOutputStream
import java.util.Properties

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, KeyedDeserializationSchema, SerializationSchema}
import org.apache.flink.util.Collector

/**
  * Created by ytaras on 10/18/16.
  */
object FlinkJoin extends App {

  run(SampleJoinConfig)

  def run(jc: JoinConfig) = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")
    properties.setProperty("auto.offset.reset", "earliest")
    properties.setProperty("enable.auto.commit", "false")

    implicit val client =
      new CachedSchemaRegistryClient("http://localhost:18081", 20)

    val dimension: DataStream[(String, GenericRecord)] = env
      .addSource(new FlinkKafkaConsumer09[(String, GenericRecord)](jc.dimensionStream, new AvroSchemaKeyed(jc.dimensionStream), properties))
    val fact: DataStream[GenericRecord] = env
      .addSource(new FlinkKafkaConsumer09[GenericRecord](jc.factStream, new AvroSchema(jc.factStream), properties))

    val joinMapper = new JoinMapper[GenericRecord, (String, GenericRecord), GenericRecord](
      (fact, dim) => SampleJoinConfig.mergeRecords(fact, dim._2)
    )
    fact.keyingBy(_.get(jc.joinKey).toString)
      .connect(dimension.keyingBy(_._1))
      .flatMap(joinMapper)
      .addSink(new FlinkKafkaProducer09[GenericRecord]("localhost:9092", jc.outputTo, new AvroSchema(jc.outputTo)))

    env.execute("Print")
  }
}

class AvroSchemaKeyed(topic: String) extends KeyedDeserializationSchema[(String, GenericRecord)] {
  lazy val schemaRegistryClient: SchemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:18081", 20)
  lazy val deserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  override def isEndOfStream(nextElement: (String, GenericRecord)): Boolean = false

  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): (String, GenericRecord) = {
    (new String(messageKey),
      deserializer.deserialize(topic, message).asInstanceOf[GenericRecord])
  }

  override def getProducedType: TypeInformation[(String, GenericRecord)] =
    implicitly[TypeInformation[(String, GenericRecord)]]

}

class AvroSchema(topic: String) extends DeserializationSchema[GenericRecord] with SerializationSchema[GenericRecord] {
  lazy val schemaRegistryClient: SchemaRegistryClient = new CachedSchemaRegistryClient("http://localhost:18081", 25)
  lazy val deserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  lazy val datumWriter = {
    val schema = Schema.parse(schemaRegistryClient.getLatestSchemaMetadata(s"$topic-value").getSchema)
    new GenericDatumWriter[GenericRecord](schema)
  }
  override def isEndOfStream(nextElement: GenericRecord): Boolean = false

  override def deserialize(message: Array[Byte]): GenericRecord = {
    deserializer.deserialize(topic, message).asInstanceOf[GenericRecord]
  }

  override def getProducedType: TypeInformation[GenericRecord] =
    implicitly[TypeInformation[GenericRecord]]

  override def serialize(element: GenericRecord): Array[Byte] =  {
    // FIXME - Due to serializaiton, confluent client doens't work well
    val baos = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().binaryEncoder(baos, null)
    datumWriter.write(element, encoder)
    encoder.flush()
    baos.close()
    baos.toByteArray
  }
}

class JoinMapper[Fact, Dim, Joined]
(
  merge: (Fact, Dim) => Joined
)(implicit ft: TypeInformation[Fact], dt: TypeInformation[Dim]) extends
  RichCoFlatMapFunction[Fact, Dim, Joined] {

  private var state: ValueState[Dim] = _
  private var buffer: ListState[Fact] = _

  override def flatMap2(value: Dim, out: Collector[Joined]): Unit = {
    state.update(value)
    buffer.get().forEach { fact =>
      val res = merge(fact, value)
      out.collect(res)
    }
    buffer.clear()
  }

  override def flatMap1(value: Fact, out: Collector[Joined]): Unit = {
    val maybeV = state.value()
    if(maybeV == null) {
      // Temporary save fact until corresponding dim value arrives
      buffer.add(value)
    } else {
      val res = merge(value, maybeV)
      out.collect(res)
    }
  }

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(new ValueStateDescriptor[Dim](
      "dimension",
      dt,
      null.asInstanceOf[Dim]
    ))

    buffer = getRuntimeContext.getListState[Fact](new ListStateDescriptor[Fact](
      "fact_buffer",
      ft
    ))
  }
}