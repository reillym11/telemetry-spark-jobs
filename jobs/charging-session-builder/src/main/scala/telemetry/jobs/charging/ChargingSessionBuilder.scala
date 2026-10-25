package telemetry.jobs.charging

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import telemetry.common.{JobArgs, Json, KafkaIO, Schemas, Spark}

object ChargingSessionBuilder {
  val InputTopic = "vehicle.charging.events.v1"
  val OutputTopic = "charging.sessions.v1"

  def step(vin: String, events: Iterator[ChargingEvent], state: GroupState[SessionState]): Iterator[ChargingSession] = {
    val (next, out, timeout) =
      Sessions.advance(state.getOption.getOrElse(Sessions.Empty), events.toList, state.getCurrentWatermarkMs())
    state.update(next)
    timeout.foreach(state.setTimeoutTimestamp)
    out.iterator
  }

  def transform(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._
    input
      .withWatermark("event_time", "10 minutes")
      .select("event_id", "vin", "event_time", "event_type", "producer_seq", "fleet", "market", "connector_type",
        "charger_type", "power_kw", "energy_added_kwh", "soc_pct", "charger_lat", "charger_lon")
      .as[ChargingEvent]
      .groupByKey(_.vin)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(step)
      .toDF()
  }

  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = Spark.session("charging-session-builder")
    val input = Json.parse(KafkaIO.read(spark, a.bootstrap, InputTopic), Schemas.ChargingEvents)
    KafkaIO.write(Json.toKafkaRecord(transform(input), col("vin")), a.bootstrap, a.output, a.checkpoint)
      .awaitTermination()
  }
}
