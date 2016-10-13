name := "kafka_join_poc"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.0.1",
  "org.apache.kafka" % "kafka-streams" % "0.10.0.1" ,
  "com.101tec" % "zkclient" % "0.8",
  "org.apache.avro" % "avro" % "1.8.1",
  "io.confluent" % "kafka-avro-serializer" % "3.0.1"
)

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "confluent" at "http://packages.confluent.io/maven/")

scalacOptions += "-Xexperimental"