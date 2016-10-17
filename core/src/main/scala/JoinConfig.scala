/**
  * Created by ytaras on 10/17/16.
  */
trait JoinConfig {
  def factStream: String
  def dimensionStream: String
  def factKey: String
  def outputTo: String

}

object SampleJoinConfig extends JoinConfig {
  override val factStream: String = "facts_part_8"

  override val factKey: String = "join_key"

  override val dimensionStream: String = "dim_part_8"

  override val outputTo: String = "merged_output"
}
