package telemetry.common

import org.scalatest.funsuite.AnyFunSuite

class GeoUtilsSpec extends AnyFunSuite {
  private def c(v: Double) = GeoUtils.coarsen(v, v)._1

  test("rounds to 2 decimals") {
    assert(c(37.12345) == 37.12)
    assert(c(37.12499) == 37.12)
    assert(c(37.12501) == 37.13)
    assert(c(-122.04321) == -122.04)
  }

  test("exact .xx5 ties go away from zero") {
    assert(c(37.125) == 37.13)
    assert(c(37.135) == 37.14)
    assert(c(-122.125) == -122.13)
    assert(c(29.995) == 30.0)
  }

  test("lat and lon are independent") {
    assert(GeoUtils.coarsen(33.44449, -112.07551) == ((33.44, -112.08)))
  }
}
