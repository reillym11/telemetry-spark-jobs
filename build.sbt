ThisBuild / organization := "com.example.telemetry"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-release", "17")

val sparkVersion = "3.5.9"
// Hadoop version bundled with spark-3.5.9-bin-hadoop3, and the AWS SDK that hadoop-aws 3.3.4 is built against
val hadoopVersion = "3.3.4"
val awsSdkVersion = "1.12.262"
val jacksonVersion = "2.15.2"

val java17Opens = Seq(
  "java.base/java.lang",
  "java.base/java.lang.invoke",
  "java.base/java.lang.reflect",
  "java.base/java.io",
  "java.base/java.net",
  "java.base/java.nio",
  "java.base/java.util",
  "java.base/java.util.concurrent",
  "java.base/java.util.concurrent.atomic",
  "java.base/sun.nio.ch",
  "java.base/sun.nio.cs",
  "java.base/sun.security.action",
  "java.base/sun.util.calendar"
).map(p => s"--add-opens=$p=ALL-UNNAMED") :+ "-Djdk.reflect.useDirectMethodHandle=false"

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
    "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
    "com.amazonaws" % "aws-java-sdk-bundle" % awsSdkVersion,
    "org.scalatest" %% "scalatest" % "3.2.20" % Test,
    "com.networknt" % "json-schema-validator" % "1.5.9" % Test
  ),
  // Spark's jackson-module-scala only accepts databind 2.15.x
  dependencyOverrides ++= Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion
  ),
  Test / fork := true,
  Test / javaOptions ++= java17Opens ++ Seq(
    "-Xmx2g",
    "-Duser.timezone=UTC",
    s"-Dcontracts.dir=${(LocalRootProject / baseDirectory).value / "contracts"}"
  ),
  Test / parallelExecution := false,
  run / fork := true,
  run / javaOptions ++= java17Opens,
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _*) => MergeStrategy.concat
    case PathList("META-INF", _*)             => MergeStrategy.discard
    case "module-info.class"                   => MergeStrategy.discard
    case _                                     => MergeStrategy.first
  }
)

lazy val common = (project in file("common"))
  .settings(commonSettings, name := "telemetry-common", assembly / skip := true)

def job(id: String) = Project(id.replace("-", ""), file(s"jobs/$id"))
  .dependsOn(common % "compile->compile;test->test")
  .settings(commonSettings, name := id, assembly / assemblyJarName := s"$id.jar")

lazy val otaStatusTracker        = job("ota-status-tracker")
lazy val chargingSessionBuilder  = job("charging-session-builder")
lazy val batteryHealthAggregator = job("battery-health-aggregator")
lazy val diagnosticsDtcRouter    = job("diagnostics-dtc-router")

lazy val root = (project in file("."))
  .aggregate(common, otaStatusTracker, chargingSessionBuilder, batteryHealthAggregator, diagnosticsDtcRouter)
  .settings(name := "telemetry-spark-jobs", publish / skip := true, assembly / aggregate := false)
