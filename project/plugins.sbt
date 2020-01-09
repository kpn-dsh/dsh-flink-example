credentials += Credentials(Path.userHome / ".ivy2" / ".klarrio-credentials")
resolvers := Seq(
  "Artifactory klarrio" at "https://klarrio.jfrog.io/klarrio/jvm-libs/"
)

addSbtPlugin("com.klarrio" % "klarrio-sbt-plugin" % "2.1.7")
