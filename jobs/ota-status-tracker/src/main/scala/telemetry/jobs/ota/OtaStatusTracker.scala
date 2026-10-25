package telemetry.jobs.ota

import java.sql.Timestamp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import telemetry.common.{JobArgs, Json, KafkaIO, Schemas, Spark}

case class OtaEvent(
    event_id: String,
    vin: String,
    event_time: Timestamp,
    producer_seq: Long,
    campaign_id: String,
    target_version: String,
    state: String,
    failure_code: String)

case class OtaLatest(
    vin: String,
    campaign_id: String,
    state: String,
    target_version: String,
    failure_code: String,
    last_event_id: String,
    last_event_time: Timestamp)

object OtaStatusTracker {
  val InputTopic = "vehicle.ota.status.v1"
  val OutputTopic = "ota.status.latest.v1"

  private def rank(e: OtaEvent): (Long, Long) = (e.event_time.getTime, e.producer_seq)

  def latest(current: Option[OtaEvent], events: Iterator[OtaEvent]): Option[OtaEvent] =
    events.foldLeft(current) {
      case (Some(cur), e) if Ordering[(Long, Long)].lteq(rank(e), rank(cur)) => Some(cur)
      case (_, e) => Some(e)
    }

  def toOutput(e: OtaEvent): OtaLatest =
    OtaLatest(e.vin, e.campaign_id, e.state, e.target_version, e.failure_code, e.event_id, e.event_time)

  def step(key: (String, String), events: Iterator[OtaEvent], state: GroupState[OtaEvent]): Iterator[OtaLatest] = {
    val before = state.getOption
    val after = latest(before, events)
    if (after == before) Iterator.empty
    else {
      after.foreach(state.update)
      after.iterator.map(toOutput)
    }
  }

  def transform(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._
    input
      .filter(col("event_type") =!= "heartbeat")
      .withWatermark("event_time", "10 minutes")
      .select("event_id", "vin", "event_time", "producer_seq", "campaign_id", "target_version", "state", "failure_code")
      .as[OtaEvent]
      .groupByKey(e => (e.vin, e.campaign_id))
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.NoTimeout())(step)
      .toDF()
  }

  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = Spark.session("ota-status-tracker")
    val input = Json.parse(KafkaIO.read(spark, a.bootstrap, InputTopic), Schemas.OtaStatus)
    val out = transform(input)
    KafkaIO
      .write(Json.toKafkaRecord(out, concat_ws("|", col("vin"), col("campaign_id"))), a.bootstrap, a.output,
        a.checkpoint, OutputMode.Update())
      .awaitTermination()
  }
}
