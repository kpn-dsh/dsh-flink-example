import sbt._

object Version {
  final val Flink  = "1.6.4"
  final val Sdk    = "0.1.0"
  final val Config = "1.4.0"
}

object Library {
  val FlinkStreaming: ModuleID = "org.apache.flink" %% "flink-streaming-scala" % Version.Flink % "provided"
  val FlinkKafka: ModuleID = "org.apache.flink" %% "flink-connector-kafka-0.11" % Version.Flink % "provided"
  val FLinkMetrics: ModuleID = "org.apache.flink" % "flink-metrics-dropwizard" % Version.Flink % "provided"
  val Config : ModuleID = "com.typesafe" % "config" % Version.Config
  val Sdk: ModuleID = "dsh-sdk" % "platform-java" % Version.Sdk exclude("org.apache.kafka", "kafka-clients" )
}
