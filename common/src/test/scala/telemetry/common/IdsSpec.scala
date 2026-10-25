package telemetry.common

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite

class IdsSpec extends AnyFunSuite {
  private val Dns = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

  test("namespace is uuid5(DNS, contracts.telemetry.example)") {
    assert(Ids.uuid5(Dns, "contracts.telemetry.example") == Ids.Namespace)
  }

  test("session_id over vin|charge_start event_id") {
    assert(Ids.sessionId("SYNM6ENAH6VAT6T2X", "0b7f6a52-3f43-4e4b-9a4e-2b0c8f7d1e55") ==
      "ad44e62b-7cd1-57b2-8eb7-0ee0c5081a34")
  }

  test("alert_id over the source event_id, case-insensitive input") {
    assert(Ids.alertId("0B7F6A52-3F43-4E4B-9A4E-2B0C8F7D1E55") == "9528d8e8-95e2-5f17-aad3-4e51fca24025")
  }
}
