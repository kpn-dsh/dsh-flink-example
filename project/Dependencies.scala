import sbt._

object Version {
  final val Flink = "1.6.1"
}

object Library {
  val FlinkStreaming: ModuleID = "org.apache.flink" %% "flink-streaming-scala" % Version.Flink % "provided"
  val FLinkMetrics: ModuleID = "org.apache.flink" % "flink-metrics-dropwizard" % Version.Flink
}
