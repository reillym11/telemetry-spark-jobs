package telemetry.common

import org.scalatest.funsuite.AnyFunSuite

class JsonSpec extends AnyFunSuite {
  import SharedSpark.spark.implicits._

  test("timestamps round-trip as UTC milliseconds with Z, nulls are kept") {
    val line = Contracts.sample("vehicle.ota.status.v1").find(_.contains("\"heartbeat\"")).get
    val parsed = Json.parse(Seq(line).toDF("value"), Schemas.OtaStatus)
    val out = Json.toJson(parsed).as[String].collect().head
    val ts = """"event_time":"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z)"""".r.findFirstMatchIn(out).map(_.group(1))
    assert(ts.isDefined && line.contains(ts.get))
    assert(out.contains("\"failure_code\":null"))
    assert(Contracts.errors("vehicle.ota.status.v1", out).isEmpty)
  }

  test("every sample record parses with no null envelope fields") {
    Schemas.byTopic.foreach { case (topic, schema) =>
      val df = Json.parse(Contracts.sample(topic).toDF("value"), schema)
      val nulls = df.filter(df("event_id").isNull || df("event_time").isNull || df("producer_seq").isNull).count()
      assert(nulls == 0, topic)
      assert(df.count() == Contracts.sample(topic).size)
    }
  }
}
