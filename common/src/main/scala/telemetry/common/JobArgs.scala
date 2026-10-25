package telemetry.common

import org.apache.spark.sql.SparkSession

case class JobArgs(bootstrap: String, checkpoint: String, output: String, s3Endpoint: Option[String])

object JobArgs {
  def parse(args: Array[String]): JobArgs = {
    val kv = args.grouped(2).collect { case Array(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
    def req(k: String) = kv.getOrElse(k, sys.error(s"missing --$k"))
    JobArgs(req("bootstrap"), req("checkpoint"), req("output"), kv.get("s3-endpoint"))
  }
}

object Spark {
  def session(app: String): SparkSession =
    SparkSession.builder()
      .appName(app)
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.shuffle.partitions", "6")
      .getOrCreate()
}
