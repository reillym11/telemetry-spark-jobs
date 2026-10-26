package telemetry.jobs.dtc

import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funsuite.AnyFunSuite
import telemetry.common.{Contracts, Ids, PipelineBehaviors, Schemas, SharedSpark}

class DiagnosticsDtcRouterSpec extends AnyFunSuite with PipelineBehaviors {

  private val template = Contracts.sample("vehicle.diagnostics.dtc.v1").find(_.contains("\"dtc\"")).get

  private def line(id: String, severity: Int, status: String, lat: String = "37.12345", lon: String = "-122.04321",
                   eventType: String = "dtc") =
    template
      .replaceAll("\"event_id\":\"[^\"]+\"", s""""event_id":"$id"""")
      .replaceAll("\"event_type\":\"[^\"]+\"", s""""event_type":"$eventType"""")
      .replaceAll("\"severity\":[0-9]+", s""""severity":$severity""")
      .replaceAll("\"status\":\"[^\"]+\"", s""""status":"$status"""")
      .replaceAll("\"lat\":[-0-9.]+", s""""lat":$lat""")
      .replaceAll("\"lon\":[-0-9.]+", s""""lon":$lon""")

  private def id(n: Int) = f"00000000-0000-4000-8000-$n%012d"

  private def route(lines: Seq[String], chunks: Int = 1) =
    SharedSpark.runStream(lines, Schemas.DiagnosticsDtc, chunks, OutputMode.Append())(DiagnosticsDtcRouter.transform)

  test("routes only active dtc events with severity >= 3") {
    val out = route(Seq(
      line(id(1), 2, "active"),
      line(id(2), 3, "active"),
      line(id(3), 5, "active"),
      line(id(4), 4, "cleared"),
      line(id(5), 1, "cleared")))
    assert(out.map(o => "\"alert_id\":\"([^\"]+)\"".r.findFirstMatchIn(o).get.group(1)).toSet ==
      Set(Ids.alertId(id(2)), Ids.alertId(id(3))))
  }

  test("duplicates within the watermark produce one alert, across batches too") {
    assert(route(Seq(line(id(7), 4, "active"), line(id(7), 4, "active"))).size == 1)
    assert(route(Seq(line(id(7), 4, "active"), line(id(8), 3, "active"), line(id(7), 4, "active")), 3).size == 2)
  }

  test("coordinates are coarsened to 2 decimals") {
    val out = route(Seq(line(id(9), 3, "active", "37.12500", "-122.12500"), line(id(10), 3, "active", "29.99512", "-95.37499")))
    assert(out.exists(o => o.contains("\"lat\":37.12") && o.contains("\"lon\":-122.12")))
    assert(out.exists(o => o.contains("\"lat\":30.0") && o.contains("\"lon\":-95.37")))
    out.foreach(o => assert(Contracts.errors(DiagnosticsDtcRouter.OutputTopic, o).isEmpty, o))
  }

  testsFor(pipeline("vehicle.diagnostics.dtc.v1", DiagnosticsDtcRouter.OutputTopic, OutputMode.Append(), 1)(
    DiagnosticsDtcRouter.transform)())
}
