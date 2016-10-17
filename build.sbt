name := "kafka_join_poc"


val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.8",
  scalacOptions += "-Xexperimental",
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    "confluent" at "http://packages.confluent.io/maven/")
)

lazy val core = project
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.8.1",
      "org.slf4j" % "slf4j-jdk14" % "1.7.21"
    )
  )
lazy val spark = project
  .dependsOn(core)
  .enablePlugins(sbtsparkpackage.SparkPackagePlugin)
  .settings(commonSettings: _*)

lazy val kafka = project
  .dependsOn(core)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "io.confluent" % "kafka-avro-serializer" % "3.0.1",
      "org.apache.kafka" % "kafka-clients" % "0.10.0.1",
      "org.apache.kafka" % "kafka-streams" % "0.10.0.1"
    )

  )

