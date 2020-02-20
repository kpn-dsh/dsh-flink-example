lazy val buildSettings = Seq(
  name                 := "flink-example",
  packageDescription   := "DSH Flink Example",
  organization         := "dsh",
  scalaVersion         := "2.11.12",
  mainClass in Compile := Some("dsh.flink.example.HelloWorld"),
  assemblyJarName in assembly := s"flink-example-${Version.Flink}.jar"
)

lazy val root = (project in file("."))
  .enablePlugins(KlarrioDefaultsPlugin)
  .settings(buildSettings)
  .settings(
    libraryDependencies ++= KlarrioDependencyGroups.loggingForApp,
    libraryDependencies ++= Seq(Library.FlinkStreaming, Library.FLinkMetrics, Library.FlinkKafka)
  )
