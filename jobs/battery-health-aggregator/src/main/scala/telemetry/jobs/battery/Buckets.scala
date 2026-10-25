package telemetry.jobs.battery

import java.sql.Timestamp

case class BatterySample(
    event_id: String,
    vin: String,
    event_time: Timestamp,
    event_type: String,
    soh_pct: Option[Double],
    cell_voltage_delta_mv: Option[Long],
    pack_temp_c: Option[Double]) {
  def t: Long = event_time.getTime
}

case class HourlyHealth(
    vin: String,
    window_start: Timestamp,
    window_end: Timestamp,
    sample_count: Int,
    soh_pct_min: BigDecimal,
    soh_pct_avg: BigDecimal,
    cell_voltage_delta_mv_min: Int,
    cell_voltage_delta_mv_max: Int,
    pack_temp_c_max: BigDecimal)

case class BucketState(pending: Seq[BatterySample], maxSeenMs: Long, emittedUntilMs: Long)

object Buckets {
  val HourMs: Long = 60L * 60 * 1000
  val GraceMs: Long = 10L * 60 * 1000

  val Empty: BucketState = BucketState(Nil, Long.MinValue, Long.MinValue)

  def windowStart(t: Long): Long = Math.floorDiv(t, HourMs) * HourMs

  private def dec(d: Double): BigDecimal = BigDecimal(d.toString)

  def aggregate(vin: String, start: Long, samples: Seq[BatterySample]): HourlyHealth = {
    val soh = samples.map(s => dec(s.soh_pct.get))
    val delta = samples.map(_.cell_voltage_delta_mv.get.toInt)
    HourlyHealth(
      vin = vin,
      window_start = new Timestamp(start),
      window_end = new Timestamp(start + HourMs),
      sample_count = samples.size,
      soh_pct_min = soh.min.setScale(2),
      soh_pct_avg = (soh.sum / samples.size).setScale(2, BigDecimal.RoundingMode.HALF_UP),
      cell_voltage_delta_mv_min = delta.min,
      cell_voltage_delta_mv_max = delta.max,
      pack_temp_c_max = samples.map(s => dec(s.pack_temp_c.get)).max.setScale(1))
  }

  /**
   * Adds `events` and emits every hour whose end + GraceMs is at or before the larger of the VIN's highest event
   * time and the watermark. Returns the new state, the emitted rows, and the next event-time timeout, if any.
   */
  def advance(state: BucketState, events: Seq[BatterySample], watermarkMs: Long)
      : (BucketState, Seq[HourlyHealth], Option[Long]) = {
    val maxSeen = (state.maxSeenMs +: events.map(_.t)).max
    val known = state.pending.map(_.event_id).toSet
    val fresh = events
      .filter(e => e.event_type == "battery_sample" && e.t >= state.emittedUntilMs && !known(e.event_id))
      .groupBy(_.event_id).values.map(_.head).toSeq
    val horizon = math.max(maxSeen, watermarkMs)
    val byHour = (state.pending ++ fresh).groupBy(s => windowStart(s.t))
    val (done, open) = byHour.partition { case (start, _) => start + HourMs + GraceMs <= horizon }
    val rows = done.toSeq.sortBy(_._1).map { case (start, ss) => aggregate(ss.head.vin, start, ss) }
    val emittedUntil = (state.emittedUntilMs +: done.keys.map(_ + HourMs).toSeq).max
    val timeout = open.keys.reduceOption(_ min _).map(_ + HourMs + GraceMs - 1)
    (BucketState(open.values.flatten.toSeq.sortBy(s => (s.t, s.event_id)), maxSeen, emittedUntil), rows, timeout)
  }
}
