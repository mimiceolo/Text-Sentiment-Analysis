name := "SentimentAnalysisStreaming"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.4.1",
  "org.apache.kafka" % "kafka-clients" % "3.6.1",
  "org.mongodb" % "mongodb-driver-sync" % "4.11.1"  // Java driver only, no Spark connector
)

// Assembly settings for fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

assembly / assemblyJarName := "sentiment-analysis-streaming-assembly.jar"
