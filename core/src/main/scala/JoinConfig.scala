import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import scala.collection.JavaConversions._

/**
  * Created by ytaras on 10/17/16.
  */
trait JoinConfig extends Serializable {
  def factStream: String
  def dimensionStream: String
  def joinKey: String
  def outputTo: String

  def factSchema: Schema
  def dimSchema: Schema
  lazy val mergedSchema: Schema = {
    println("Constructing schema")
    val fields = (factSchema.getFields ++ dimSchema.getFields).groupBy(_.name())
      .map { case (_, v) => v.head }.map { f =>
      new Schema.Field(f.name(), f.schema, f.doc(), f.defaultVal())
    }
    Schema.createRecord("merged", null, null, false, fields.toList)
  }

  def mergeRecords(rec1: GenericRecord, rec2: GenericRecord): GenericRecord = {
    def addAll(from: GenericRecord, fromSchema: Schema, to: GenericRecord) =
      fromSchema.getFields.foreach { f => to.put(f.name, from.get(f.name)) }
    val result = new GenericData.Record(mergedSchema)
    if(rec1 != null)
      addAll(rec1, factSchema, result)
    if(rec2 != null)
      addAll(rec2, dimSchema, result)
    result
  }

}

object SampleJoinConfig extends JoinConfig {

  lazy val client = new CachedSchemaRegistryClient("http://localhost:18081", 20)

  override val factStream: String = "fact_part_8"

  override val joinKey: String = "join_key"

  override val dimensionStream: String = "dim_part_8"

  override val outputTo: String = "merged_output"

  override lazy val factSchema: Schema =
    new Parser().parse(
      client.getLatestSchemaMetadata(s"$factStream-value").getSchema
    )

  override lazy val dimSchema: Schema =
    new Parser().parse(
      client.getLatestSchemaMetadata(s"$dimensionStream-value").getSchema
    )
}
