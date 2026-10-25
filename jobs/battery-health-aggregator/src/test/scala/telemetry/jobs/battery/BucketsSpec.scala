package telemetry.jobs.battery

import java.nio.file.Files
import java.sql.Timestamp

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funsuite.AnyFunSuite
import telemetry.common.{Contracts, Json, PipelineBehaviors, Schemas, SharedSpark}

class BucketsSpec extends AnyFunSuite with PipelineBehaviors {
  private val Vin = "SYNTESTVIN0000001"
  private val Min = 60L * 1000
  private val H0 = 1790812800000L

  private var n = 0
  private def s(minute: Long, soh: Double = 95.0, delta: Long = 20, temp: Double = 30.0, kind: String = "battery_sample") = {
    n += 1
    val hb = kind == "heartbeat"
    BatterySample(f"00000000-0000-4000-8000-$n%012d", Vin, new Timestamp(H0 + minute * Min), kind,
      if (hb) None else Some(soh), if (hb) None else Some(delta), if (hb) None else Some(temp))
  }

  private def emit(batches: Seq[BatterySample]*): Seq[HourlyHealth] =
    batches.foldLeft((Buckets.Empty, Vector.empty[HourlyHealth])) { case ((st, acc), b) =>
      val (next, rows, _) = Buckets.advance(st, b, Long.MinValue)
      (next, acc ++ rows)
    }._2

  test("samples are bucketed by UTC hour of event_time") {
    val rows = emit(Seq(s(0), s(10), s(59), s(60), s(119)), Seq(s(240, kind = "heartbeat")))
    assert(rows.map(r => (r.window_start.getTime, r.sample_count)) == Seq((H0, 3), (H0 + 60 * Min, 2)))
    assert(rows.head.window_end.getTime == H0 + 60 * Min)
  }

  test("an hour is emitted once an event is at least 10 minutes past its end") {
    assert(emit(Seq(s(5), s(69))).isEmpty)
    assert(emit(Seq(s(5), s(70))).map(_.window_start.getTime) == Seq(H0))
    assert(emit(Seq(s(5)), Seq(s(70, kind = "heartbeat"))).size == 1)
  }

  test("a late sample inside the grace period joins its hour") {
    val rows = emit(Seq(s(50), s(65)), Seq(s(55, soh = 90.0)), Seq(s(130, kind = "heartbeat")))
    assert(rows.head.sample_count == 2 && rows.head.soh_pct_min == BigDecimal("90.00"))
  }

  test("duplicates are counted once, before and after emission") {
    val a = s(1)
    val rows = emit(Seq(a, a, s(2)), Seq(a, s(80)), Seq(a), Seq(s(200, kind = "heartbeat")))
    assert(rows.map(_.sample_count) == Seq(2, 1))
  }

  test("soh_pct_avg is the exact average rounded HALF_UP to 2 decimals") {
    def avg(xs: Double*) = Buckets.aggregate(Vin, H0, xs.map(x => s(1, soh = x))).soh_pct_avg
    assert(avg(95.01, 95.02) == BigDecimal("95.02"))
    assert(avg(95.01, 95.02, 95.02) == BigDecimal("95.02"))
    assert(avg(95.01, 95.01, 95.02) == BigDecimal("95.01"))
    assert(avg(90.1, 90.2, 90.2, 90.2, 90.2, 90.2, 90.2, 90.2) == BigDecimal("90.19"))
    assert(avg(0.1, 0.2) == BigDecimal("0.15"))
  }

  test("min, max and temperature aggregates") {
    val r = Buckets.aggregate(Vin, H0, Seq(s(1, 95.5, 12, 31.4), s(2, 95.25, 48, 33.0), s(3, 96.0, 7, 29.9)))
    assert(r.soh_pct_min == BigDecimal("95.25") && r.cell_voltage_delta_mv_min == 7)
    assert(r.cell_voltage_delta_mv_max == 48 && r.pack_temp_c_max == BigDecimal("33.0"))
  }

  test("the event-time timeout is hour end + 10 minutes") {
    val (st, rows, timeout) = Buckets.advance(Buckets.Empty, Seq(s(5)), Long.MinValue)
    assert(rows.isEmpty && timeout.contains(H0 + 70 * Min - 1))
    assert(Buckets.advance(st, Nil, H0 + 70 * Min)._2.size == 1)
  }

  testsFor(pipeline("vehicle.battery.health.v1", "battery.health.hourly", OutputMode.Append(), 63)(
    BatteryHealthAggregator.transform)())

  test("the parquet sink writes the x-parquet-type physical types, partitioned by date") {
    val spark = SharedSpark.spark
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    val dir = Files.createTempDirectory("battery")
    val input = MemoryStream[String](900, spark.sqlContext)(Encoders.STRING)
    val out = BatteryHealthAggregator.transform(Json.parse(input.toDF(), Schemas.BatteryHealth))
    input.addData(Contracts.sample("vehicle.battery.health.v1"))
    val q = BatteryHealthAggregator.write(out, dir.resolve("out").toString, dir.resolve("chk").toString)
    q.awaitTermination()

    val partitions = Files.list(dir.resolve("out")).iterator().asScala.map(_.getFileName.toString)
      .filter(_.startsWith("date=")).toSet
    assert(partitions == Set("date=2026-10-01"))
    val file = Files.walk(dir.resolve("out")).iterator().asScala.find(_.toString.endsWith(".parquet")).get
    val reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(file.toUri), spark.sparkContext.hadoopConfiguration))
    val types = try reader.getFooter.getFileMetaData.getSchema.getFields.asScala
      .map(f => f.getName -> f.asPrimitiveType()).toMap finally reader.close()

    val props = Contracts.schemaJson("battery.health.hourly").get("properties")
    props.fieldNames().asScala.filter(_ != "date").foreach { name =>
      val t = types(name)
      val physical = props.get(name).get("x-parquet-type").asText match {
        case "STRING" => "BINARY/STRING"
        case "INT32" => "INT32/null"
        case "TIMESTAMP_MILLIS_UTC" => "INT64/TIMESTAMP(MILLIS,true)"
        case d if d.startsWith("DECIMAL") => s"INT32|INT64/${d.toUpperCase}"
      }
      val actual = s"${t.getPrimitiveTypeName}/${t.getLogicalTypeAnnotation}"
      val Array(phys, logical) = physical.split("/")
      assert(phys.split('|').contains(t.getPrimitiveTypeName.name()), s"$name: $actual")
      assert(String.valueOf(t.getLogicalTypeAnnotation).toUpperCase.replace(" ", "") == logical.toUpperCase.replace(" ", ""),
        s"$name: $actual")
    }
    assert(spark.read.parquet(dir.resolve("out").toString).count() == 63)
  }
}
