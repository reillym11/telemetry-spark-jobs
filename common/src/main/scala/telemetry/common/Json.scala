package telemetry.common

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object Json {
  val TimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

  val options: Map[String, String] = Map(
    "timestampFormat" -> TimestampFormat,
    "dateFormat" -> "yyyy-MM-dd",
    "ignoreNullFields" -> "false"
  )

  def parse(raw: DataFrame, schema: StructType): DataFrame =
    raw
      .select(from_json(col("value").cast("string"), schema, options).as("e"))
      .select("e.*")

  def toKafkaRecord(df: DataFrame, key: Column): DataFrame =
    df.select(
      key.cast("string").as("key"),
      to_json(struct(df.columns.map(c => col(c)): _*), options).as("value")
    )

  def toJson(df: DataFrame): DataFrame =
    df.select(to_json(struct(df.columns.map(c => col(c)): _*), options).as("value"))
}
