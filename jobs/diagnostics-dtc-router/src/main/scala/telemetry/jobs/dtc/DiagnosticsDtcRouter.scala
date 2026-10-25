package telemetry.jobs.dtc

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import telemetry.common.{GeoUtils, Ids, JobArgs, Json, KafkaIO, Schemas, Spark}

object DiagnosticsDtcRouter {
  val InputTopic = "vehicle.diagnostics.dtc.v1"
  val OutputTopic = "service.dtc.alerts.v1"
  val MinSeverity = 3

  private val alertId = udf((eventId: String) => Ids.alertId(eventId))
  private val coarse = udf((lat: Double, lon: Double) => GeoUtils.coarsen(lat, lon))

  def routable: Column =
    col("event_type") === "dtc" && col("severity") >= MinSeverity && col("status") === "active"

  def transform(input: DataFrame): DataFrame =
    input
      .filter(routable)
      .withWatermark("event_time", "10 minutes")
      .dropDuplicatesWithinWatermark("event_id")
      .withColumn("coarse", coarse(col("lat"), col("lon")))
      .select(
        alertId(col("event_id")).as("alert_id"),
        col("vin"),
        col("dtc_code"),
        col("severity"),
        col("ecu"),
        col("event_time"),
        col("coarse._1").as("lat"),
        col("coarse._2").as("lon"))

  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = Spark.session("diagnostics-dtc-router")
    val input = Json.parse(KafkaIO.read(spark, a.bootstrap, InputTopic), Schemas.DiagnosticsDtc)
    KafkaIO.write(Json.toKafkaRecord(transform(input), col("vin")), a.bootstrap, a.output, a.checkpoint)
      .awaitTermination()
  }
}
