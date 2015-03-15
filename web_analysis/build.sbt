name := "web_analysis"

version := "0.0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.3.0" % "provided"

//libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "1.8.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)