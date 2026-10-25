package telemetry.jobs.ota

import java.sql.Timestamp

import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funsuite.AnyFunSuite
import telemetry.common.PipelineBehaviors

class OtaStatusTrackerSpec extends AnyFunSuite with PipelineBehaviors {

  private def ev(id: String, t: Long, seq: Long, state: String, code: String = null) =
    OtaEvent(id, "SYNTESTVIN0000001", new Timestamp(t), seq, "OTA-2026.10.1", "2026.10.1", state, code)

  private val downloaded = ev("e1", 1000, 1, "downloaded")
  private val installed = ev("e2", 2000, 2, "installed")

  test("newer event wins") {
    assert(OtaStatusTracker.latest(Some(downloaded), Iterator(installed)).contains(installed))
  }

  test("a late older event never overwrites a newer one") {
    assert(OtaStatusTracker.latest(Some(installed), Iterator(downloaded)).contains(installed))
    assert(OtaStatusTracker.latest(None, Iterator(installed, downloaded)).contains(installed))
  }

  test("producer_seq breaks event_time ties") {
    val a = ev("a", 5000, 3, "downloaded")
    val b = ev("b", 5000, 4, "failed", "E_TIMEOUT")
    assert(OtaStatusTracker.latest(None, Iterator(b, a)).contains(b))
    assert(OtaStatusTracker.latest(None, Iterator(a, b)).contains(b))
  }

  test("a duplicate of the current event is not a change") {
    assert(OtaStatusTracker.latest(Some(installed), Iterator(installed.copy())).contains(installed))
  }

  test("output carries the winning event's id and time") {
    val out = OtaStatusTracker.toOutput(installed)
    assert(out.last_event_id == "e2" && out.last_event_time == installed.event_time && out.state == "installed")
  }

  private val lastPerKey: Seq[String] => Seq[String] = rows =>
    rows.groupBy(r => "\"vin\":\"[^\"]+\"".r.findFirstIn(r).get + "\"campaign_id\":\"[^\"]+\"".r.findFirstIn(r).get)
      .values.map(_.last).toSeq.sorted

  testsFor(pipeline("vehicle.ota.status.v1", OtaStatusTracker.OutputTopic, OutputMode.Update(), 12)(
    OtaStatusTracker.transform)(lastPerKey))
}
