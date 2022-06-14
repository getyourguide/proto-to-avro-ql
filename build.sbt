name := "proto-to-avro-ql"
ThisBuild / organization := "com.getyourguide"
// for now - the versioning major version follows google ads major version
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.12.13"
ThisBuild / publish / skip := true

val commonSettings = Seq(
  publishMavenStyle := true,
  publishConfiguration := publishConfiguration.value.withOverwrite(true)
)

val GrpcVersion = "1.36.0"
// Warning: in 2.0, lists are written differently. If you bump this to 2.0, you need to update lists accordingly
val ParquetVersion = "1.10.0"

lazy val root = project
  .in(file("."))
  .aggregate(
    protoToAvro,
    runner
  )

lazy val protoToAvro = project
  .in(file("src/proto-avro"))
  .settings(commonSettings)
  .settings(
    name := "proto-to-avro",
    libraryDependencies ++= Seq(
      "org.apache.parquet" % "parquet-avro" % ParquetVersion,
      "org.apache.parquet" % "parquet-common" % ParquetVersion,
      "org.apache.parquet" % "parquet-hadoop" % ParquetVersion,
      "org.apache.hadoop" % "hadoop-common" % "3.3.1",
      "org.apache.avro" % "avro" % "1.11.0",
      "org.apache.avro" % "avro-protobuf" % "1.10.2",
      "org.scalatest" %% "scalatest" % "3.2.7" % Test
    ),
    publish / skip := false
  )

lazy val runner = project
  .in(file("src/runner"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.github.pureconfig" %% "pureconfig" % "0.14.0",
      "org.xerial.snappy" % "snappy-java" % "1.1.7.5",
      "com.google.api-ads" % "google-ads" % "12.0.0"
    ),
    publish / skip := true
  )
  .dependsOn(protoToAvro)
