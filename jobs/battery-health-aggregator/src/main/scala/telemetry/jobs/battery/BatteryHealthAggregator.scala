package telemetry.jobs.battery

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery, Trigger}
import telemetry.common.{JobArgs, Json, KafkaIO, Schemas, Spark}

object BatteryHealthAggregator {
  val InputTopic = "vehicle.battery.health.v1"

  def step(vin: String, events: Iterator[BatterySample], state: GroupState[BucketState]): Iterator[HourlyHealth] = {
    val (next, out, timeout) =
      Buckets.advance(state.getOption.getOrElse(Buckets.Empty), events.toList, state.getCurrentWatermarkMs())
    state.update(next)
    timeout.foreach(state.setTimeoutTimestamp)
    out.iterator
  }

  def transform(input: DataFrame): DataFrame = {
    import input.sparkSession.implicits._
    input
      .withWatermark("event_time", "10 minutes")
      .select("event_id", "vin", "event_time", "event_type", "soh_pct", "cell_voltage_delta_mv", "pack_temp_c")
      .as[BatterySample]
      .groupByKey(_.vin)
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(step)
      .select(
        col("vin"),
        to_date(col("window_start")).as("date"),
        col("window_start"),
        col("window_end"),
        col("sample_count"),
        col("soh_pct_min").cast("decimal(5,2)").as("soh_pct_min"),
        col("soh_pct_avg").cast("decimal(5,2)").as("soh_pct_avg"),
        col("cell_voltage_delta_mv_min"),
        col("cell_voltage_delta_mv_max"),
        col("pack_temp_c_max").cast("decimal(4,1)").as("pack_temp_c_max"))
  }

  def configureS3(spark: SparkSession, endpoint: Option[String]): Unit = {
    val conf = spark.sparkContext.hadoopConfiguration
    endpoint.foreach { e =>
      conf.set("fs.s3a.endpoint", e)
      conf.set("fs.s3a.connection.ssl.enabled", e.startsWith("https").toString)
    }
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
  }

  def write(out: DataFrame, path: String, checkpoint: String): StreamingQuery =
    out.writeStream
      .format("parquet")
      .option("path", path)
      .option("checkpointLocation", checkpoint)
      .partitionBy("date")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.AvailableNow())
      .start()

  def main(args: Array[String]): Unit = {
    val a = JobArgs.parse(args)
    val spark = Spark.session("battery-health-aggregator")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
    configureS3(spark, a.s3Endpoint)
    val input = Json.parse(KafkaIO.read(spark, a.bootstrap, InputTopic), Schemas.BatteryHealth)
    write(transform(input), a.output, a.checkpoint).awaitTermination()
  }
}
