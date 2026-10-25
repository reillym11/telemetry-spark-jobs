package telemetry.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}

object KafkaIO {
  def read(spark: SparkSession, bootstrap: String, topic: String): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

  def write(records: DataFrame, bootstrap: String, topic: String, checkpoint: String,
            mode: OutputMode = OutputMode.Append()): StreamingQuery =
    records.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("topic", topic)
      .option("checkpointLocation", checkpoint)
      .outputMode(mode)
      .trigger(Trigger.AvailableNow())
      .start()
}
