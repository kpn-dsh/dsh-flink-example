import sbt._

object Version {
  final val Flink = "1.6.1"

}

object Library {
  val FlinkStreaming: ModuleID = "org.apache.flink" %% "flink-streaming-scala" % Version.Flink exclude("log4j", "log4j") exclude("org.slf4j", "slf4j-log4j12")
  val FLinkMetrics: ModuleID = "org.apache.flink" % "flink-metrics-dropwizard" % Version.Flink
}
