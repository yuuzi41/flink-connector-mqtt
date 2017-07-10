name := "flink-connector-mqtt"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.11"

resolvers += "Paho Releases" at "https://repo.eclipse.org/content/repositories/paho-releases"

libraryDependencies ++= Seq(
 // flink
  "org.apache.flink" %% "flink-streaming-scala" % "1.3.1",
  "org.apache.flink" %% "flink-streaming-scala" % "1.3.1" % "test",
  "org.apache.flink" %% "flink-test-utils" % "1.3.1" % "test",
  "org.apache.flink" %% "flink-tests" % "1.3.1" % "test",
  "org.apache.flink" %% "flink-runtime" % "1.3.1" % "test",

  // paho mqtt
  "org.eclipse.paho" % "mqtt-client" % "0.4.0", 

  // junit
  "junit" % "junit" % "4.12" % "test",

  // powermockito
  "org.powermock" % "powermock-module-junit4" % "1.5.5" % "test",
  "org.powermock" % "powermock-api-mockito" % "1.5.5" % "test"
)

