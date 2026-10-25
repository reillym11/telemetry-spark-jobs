package telemetry.common

import scala.math.BigDecimal.RoundingMode

object GeoUtils {
  def coarsen(lat: Double, lon: Double): (Double, Double) =
    (to2(lat), to2(lon))

  private def to2(value: Double): Double =
    BigDecimal(value.toString).setScale(2, RoundingMode.HALF_UP).toDouble
}
