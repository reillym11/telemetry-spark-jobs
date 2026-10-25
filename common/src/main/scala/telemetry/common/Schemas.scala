package telemetry.common

import org.apache.spark.sql.types._

object Schemas {

  private def envelope: Seq[StructField] = Seq(
    StructField("event_id", StringType, nullable = false),
    StructField("vin", StringType, nullable = false),
    StructField("event_time", TimestampType, nullable = false),
    StructField("event_type", StringType, nullable = false),
    StructField("producer_seq", LongType, nullable = false),
    StructField("schema_version", StringType, nullable = false),
    StructField("vehicle_model", StringType, nullable = false),
    StructField("fleet", StringType, nullable = false),
    StructField("market", StringType, nullable = false),
    StructField("firmware_version", StringType, nullable = false)
  )

  private def topic(fields: StructField*): StructType = StructType(envelope ++ fields)

  val ChargingEvents: StructType = topic(
    StructField("connector_type", StringType),
    StructField("charger_type", StringType),
    StructField("power_kw", DoubleType),
    StructField("energy_added_kwh", DoubleType),
    StructField("soc_pct", DoubleType),
    StructField("charger_lat", DoubleType),
    StructField("charger_lon", DoubleType)
  )

  val OtaStatus: StructType = topic(
    StructField("campaign_id", StringType),
    StructField("from_version", StringType),
    StructField("target_version", StringType),
    StructField("state", StringType),
    StructField("failure_code", StringType)
  )

  val BatteryHealth: StructType = topic(
    StructField("soh_pct", DoubleType),
    StructField("cell_voltage_min_v", DoubleType),
    StructField("cell_voltage_max_v", DoubleType),
    StructField("cell_voltage_delta_mv", LongType),
    StructField("pack_temp_c", DoubleType),
    StructField("soc_pct", DoubleType),
    StructField("odometer_km", LongType)
  )

  val DiagnosticsDtc: StructType = topic(
    StructField("dtc_code", StringType),
    StructField("severity", LongType),
    StructField("ecu", StringType),
    StructField("status", StringType),
    StructField("lat", DoubleType),
    StructField("lon", DoubleType),
    StructField("odometer_km", LongType)
  )

  val byTopic: Map[String, StructType] = Map(
    "vehicle.charging.events.v1" -> ChargingEvents,
    "vehicle.ota.status.v1" -> OtaStatus,
    "vehicle.battery.health.v1" -> BatteryHealth,
    "vehicle.diagnostics.dtc.v1" -> DiagnosticsDtc
  )
}
