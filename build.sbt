name := "kafka_join_poc"


val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.8",
  scalacOptions += "-Xexperimental",
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    "spark_packages" at "https://dl.bintray.com/spark-packages/maven/",
    "confluent" at "http://packages.confluent.io/maven/"
  )
)

lazy val core = project
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.8.1",
      "io.confluent" % "kafka-avro-serializer" % "3.0.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.5"
    )
  )
lazy val spark = project
  .dependsOn(core)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.0.1",
      "org.apache.spark" %% "spark-sql" % "2.0.1",
      "org.apache.spark" %% "spark-streaming" % "2.0.1",
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1",
      "databricks" % "spark-avro" % "3.0.1-s_2.11"
    )
  )

lazy val kafka = project
  .dependsOn(core)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "0.10.0.1",
      "org.apache.kafka" % "kafka-streams" % "0.10.0.1"
    )
  )

lazy val flink = project
  .dependsOn(core)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-streaming-scala" % "1.1.3" % "provided",
      "org.apache.flink" %% "flink-scala" % "1.1.3" % "provided",
      "org.apache.flink" %% "flink-clients" % "1.1.3" % "provided",
      "org.apache.flink" %% "flink-connector-kafka-0.9" % "1.1.3"
    )
  )

