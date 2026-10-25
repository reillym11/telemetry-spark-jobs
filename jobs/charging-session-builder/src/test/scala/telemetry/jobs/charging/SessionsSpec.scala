package telemetry.jobs.charging

import java.sql.Timestamp

import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funsuite.AnyFunSuite
import telemetry.common.{Ids, PipelineBehaviors}

class SessionsSpec extends AnyFunSuite with PipelineBehaviors {
  private val Vin = "SYNTESTVIN0000001"
  private val Min = 60L * 1000
  private val T0 = 1790812800000L

  private var n = 0
  private def ev(kind: String, minute: Double, energy: Double = 0.0, power: Double = 50.0, soc: Double = 50.0,
                 seq: Long = 0): ChargingEvent = {
    n += 1
    val hb = kind == "heartbeat"
    def opt[A](a: A) = if (hb) None else Some(a)
    ChargingEvent(f"00000000-0000-4000-8000-$n%012d", Vin, new Timestamp(T0 + (minute * Min).toLong), kind,
      if (seq > 0) seq else n.toLong, "consumer", "us-bay-area", if (hb) null else "NACS",
      if (hb) null else "dc_fast", opt(power), opt(energy), opt(soc), opt(37.12345), opt(-122.04321))
  }

  private def run(batches: Seq[ChargingEvent]*): Seq[ChargingSession] = {
    val (state, out) = batches.foldLeft((Sessions.Empty, Vector.empty[ChargingSession])) { case ((s, acc), b) =>
      val (next, emitted, _) = Sessions.advance(s, b, Long.MinValue)
      (next, acc ++ emitted)
    }
    assert(state.pending.forall(_.vin == Vin))
    out
  }

  test("a gap of more than 30 minutes splits sessions; exactly 30 minutes does not") {
    val a = Seq(ev("charge_start", 0), ev("charge_progress", 30, 5), ev("charge_progress", 60, 9))
    val b = Seq(ev("charge_start", 91), ev("charge_progress", 100, 3))
    val out = run(a ++ b :+ ev("heartbeat", 200))
    assert(out.map(_.event_count) == Seq(3, 2))
    assert(out.map(_.close_reason) == Seq("gap_timeout", "gap_timeout"))
    assert(out.head.end_time.getTime == T0 + 60 * Min)
  }

  test("charge_stop sets close_reason but does not emit until the gap passes") {
    val start = ev("charge_start", 0, soc = 40)
    val stop = ev("charge_stop", 20, energy = 12.5, power = 0, soc = 55)
    assert(run(Seq(start, ev("charge_progress", 10, 6, 120, 48), stop)).isEmpty)
    val out = run(Seq(start, ev("charge_progress", 10, 6, 120, 48), stop), Seq(ev("heartbeat", 51)))
    assert(out.size == 1)
    val s = out.head
    assert(s.close_reason == "charge_stop")
    assert(s.start_soc_pct == 40 && s.end_soc_pct == 55 && s.energy_added_kwh == 12.5 && s.peak_power_kw == 120)
    assert(s.start_time == start.event_time && s.end_time == stop.event_time)
  }

  test("a late event joins the right session") {
    val start = ev("charge_start", 0)
    val p1 = ev("charge_progress", 5, 2, 60)
    val p2 = ev("charge_progress", 10, 4, 150)
    val stop = ev("charge_stop", 15, 6, 0)
    val out = run(Seq(start, p1), Seq(stop), Seq(p2), Seq(ev("heartbeat", 120)))
    assert(out.size == 1 && out.head.event_count == 4 && out.head.peak_power_kw == 150)
  }

  test("a late charge_start still starts the session") {
    val start = ev("charge_start", 0, soc = 33)
    val out = run(Seq(ev("charge_progress", 5, 2)), Seq(start, ev("charge_stop", 9, 3)), Seq(ev("heartbeat", 60)))
    assert(out.size == 1 && out.head.event_count == 3 && out.head.start_soc_pct == 33)
    assert(out.head.session_id == Ids.sessionId(Vin, start.event_id))
  }

  test("duplicates are ignored, before and after the session is emitted") {
    val start = ev("charge_start", 0)
    val p = ev("charge_progress", 5, 2)
    val stop = ev("charge_stop", 9, 3)
    val out = run(Seq(start, p, p), Seq(p, stop, stop), Seq(ev("heartbeat", 60)), Seq(stop, p))
    assert(out.size == 1 && out.head.event_count == 3)
  }

  test("a missing charge_stop gives gap_timeout, ended at the last event") {
    val out = run(Seq(ev("charge_start", 0), ev("charge_progress", 10, 4, soc = 61)), Seq(ev("heartbeat", 41)))
    assert(out.size == 1 && out.head.close_reason == "gap_timeout")
    assert(out.head.end_time.getTime == T0 + 10 * Min && out.head.end_soc_pct == 61)
  }

  test("session_id is UUIDv5 over vin|charge_start event_id") {
    val start = ev("charge_start", 0)
    val out = run(Seq(start, ev("charge_stop", 5, 1)), Seq(ev("heartbeat", 40)))
    assert(out.head.session_id == Ids.sessionId(Vin, start.event_id))
    assert(out.head.charger_lat == 37.12345 && out.head.charger_lon == -122.04321)
  }

  test("events outside a session are ignored") {
    val out = run(Seq(ev("charge_progress", 0, 1), ev("charge_start", 40), ev("charge_stop", 50, 2),
      ev("charge_progress", 55, 3)), Seq(ev("heartbeat", 200)))
    assert(out.size == 1 && out.head.event_count == 2)
  }

  test("the watermark closes a session and the timeout is last event + 30 minutes") {
    val (s1, out1, timeout) = Sessions.advance(Sessions.Empty, Seq(ev("charge_start", 0), ev("charge_progress", 4, 1)),
      Long.MinValue)
    assert(out1.isEmpty && timeout.contains(T0 + 34 * Min))
    val (_, early, _) = Sessions.advance(s1, Nil, T0 + 34 * Min)
    assert(early.isEmpty)
    val (s2, out2, t2) = Sessions.advance(s1, Nil, T0 + 34 * Min + 1)
    assert(out2.size == 1 && s2.pending.isEmpty && t2.isEmpty)
  }

  testsFor(pipeline("vehicle.charging.events.v1", ChargingSessionBuilder.OutputTopic, OutputMode.Append(), 11)(
    ChargingSessionBuilder.transform)())
}
