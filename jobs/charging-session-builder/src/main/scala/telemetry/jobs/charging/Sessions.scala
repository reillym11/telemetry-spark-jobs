package telemetry.jobs.charging

import java.sql.Timestamp

import scala.collection.mutable.ArrayBuffer

case class ChargingEvent(
    event_id: String,
    vin: String,
    event_time: Timestamp,
    event_type: String,
    producer_seq: Long,
    fleet: String,
    market: String,
    connector_type: String,
    charger_type: String,
    power_kw: Option[Double],
    energy_added_kwh: Option[Double],
    soc_pct: Option[Double],
    charger_lat: Option[Double],
    charger_lon: Option[Double]) {
  def t: Long = event_time.getTime
}

case class ChargingSession(
    session_id: String,
    vin: String,
    fleet: String,
    market: String,
    connector_type: String,
    charger_type: String,
    start_time: Timestamp,
    end_time: Timestamp,
    close_reason: String,
    energy_added_kwh: Double,
    peak_power_kw: Double,
    start_soc_pct: Double,
    end_soc_pct: Double,
    event_count: Long,
    charger_lat: Double,
    charger_lon: Double)

/** Per-VIN state: events not yet emitted, highest event time seen (heartbeats too), end of the last emitted session. */
case class SessionState(pending: Seq[ChargingEvent], maxSeenMs: Long, emittedUntilMs: Long)

object Sessions {
  val GapMs: Long = 30L * 60 * 1000

  val Empty: SessionState = SessionState(Nil, Long.MinValue, Long.MinValue)

  private[charging] case class Run(events: Seq[ChargingEvent], stopped: Boolean) {
    def last: Long = events.last.t
  }

  private def order(es: Seq[ChargingEvent]) = es.sortBy(e => (e.t, e.producer_seq))

  private[charging] def segment(events: Seq[ChargingEvent]): (Seq[Run], Seq[ChargingEvent]) = {
    val runs = ArrayBuffer.empty[Run]
    val stray = ArrayBuffer.empty[ChargingEvent]
    var open: Option[ArrayBuffer[ChargingEvent]] = None
    def close(stopped: Boolean): Unit = { open.foreach(b => runs += Run(b.toList, stopped)); open = None }
    order(events).foreach { e =>
      e.event_type match {
        case "charge_start" =>
          close(stopped = false)
          open = Some(ArrayBuffer(e))
        case _ =>
          open match {
            case Some(b) if e.t - b.last.t <= GapMs =>
              b += e
              if (e.event_type == "charge_stop") close(stopped = true)
            case Some(_) =>
              close(stopped = false)
              stray += e
            case None =>
              stray += e
          }
      }
    }
    close(stopped = false)
    (runs.toList, stray.toList)
  }

  def build(run: Run): ChargingSession = {
    val start = run.events.head
    val last = run.events.last
    ChargingSession(
      session_id = telemetry.common.Ids.sessionId(start.vin, start.event_id),
      vin = start.vin,
      fleet = start.fleet,
      market = start.market,
      connector_type = start.connector_type,
      charger_type = start.charger_type,
      start_time = start.event_time,
      end_time = last.event_time,
      close_reason = if (run.stopped) "charge_stop" else "gap_timeout",
      energy_added_kwh = last.energy_added_kwh.get,
      peak_power_kw = run.events.flatMap(_.power_kw).max,
      start_soc_pct = start.soc_pct.get,
      end_soc_pct = last.soc_pct.get,
      event_count = run.events.map(_.event_id).distinct.size.toLong,
      charger_lat = start.charger_lat.get,
      charger_lon = start.charger_lon.get)
  }

  /**
   * Adds `events` (any type, any order) and emits every session whose last event is more than GapMs before
   * `horizon`, the larger of the VIN's highest event time and the watermark.
   * Returns the new state, the emitted sessions, and the next event-time timeout, if any.
   */
  def advance(state: SessionState, events: Seq[ChargingEvent], watermarkMs: Long)
      : (SessionState, Seq[ChargingSession], Option[Long]) = {
    val maxSeen = (state.maxSeenMs +: events.map(_.t)).max
    val known = state.pending.map(_.event_id).toSet
    val fresh = events
      .filter(e => e.event_type != "heartbeat" && e.t > state.emittedUntilMs && !known(e.event_id))
      .groupBy(_.event_id).values.map(_.head).toSeq
    val pending = state.pending ++ fresh
    val horizon = math.max(maxSeen, watermarkMs)

    val (runs, stray) = segment(pending)
    val (done, open) = runs.partition(r => horizon - r.last > GapMs)
    val keptStray = stray.filter(e => horizon - e.t <= GapMs)
    val emittedUntil = (state.emittedUntilMs +: done.map(_.last)).max
    val next = SessionState(order(open.flatMap(_.events) ++ keptStray), maxSeen, emittedUntil)
    val timeout = (open.map(_.last) ++ keptStray.map(_.t)).reduceOption(_ min _).map(_ + GapMs)
    (next, done.map(build), timeout)
  }
}
