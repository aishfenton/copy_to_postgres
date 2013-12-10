name := "CopyToPostgres"

version := "0.1"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.7.5",
  "org.postgresql" % "postgresql" % "9.3-1100-jdbc41"
)

