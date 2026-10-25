package telemetry.common

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

object SharedSpark {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("telemetry-tests")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.streaming.checkpointLocation", java.nio.file.Files.createTempDirectory("chk").toString)
    .getOrCreate()

  private val queries = new AtomicInteger()

  /** Feeds `lines` through `transform` in `chunks` consecutive micro-batches and returns the output as JSON. */
  def runStream(lines: Seq[String], schema: StructType, chunks: Int, mode: OutputMode)
               (transform: DataFrame => DataFrame): Seq[String] = {
    val input = MemoryStream[String](queries.incrementAndGet(), spark.sqlContext)(Encoders.STRING)
    val name = s"out_${queries.incrementAndGet()}"
    val query = transform(Json.parse(input.toDF(), schema))
      .writeStream.format("memory").queryName(name).outputMode(mode).start()
    try {
      val size = math.max(1, math.ceil(lines.size.toDouble / chunks).toInt)
      lines.grouped(size).foreach { chunk =>
        input.addData(chunk)
        query.processAllAvailable()
      }
      query.processAllAvailable()
    } finally query.stop()
    Json.toJson(spark.table(name)).collect().map(_.getString(0)).toSeq
  }
}
