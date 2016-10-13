/**
  * Created by ytaras on 10/13/16.
  */
object Main extends App {
  val (dir: String, doFact: Boolean, doDim: Boolean) = args match {
    case Array(d) => (d, true, true)
    case Array(d, x) if x == "all" => (d, true, true)
    case Array(d, x) if x == "dim" => (d, false, true)
    case Array(d, x) if x == "fact" => (d, true, false)
    case _ =>
      sys.error(
      """
        |Expected format: avro_dir [fact|dim|all]
      """.stripMargin)
  }

  if (doFact) {
    val input = s"$dir/fact"
    println(s"Importing facts in directory $input")
    ImportFacts.run(input)
  }

  if (doDim) {
    val input = s"$dir/dimension"
    println(s"Importing dimension in directory $input")
    ImportDimension.run(input)
  }
}
