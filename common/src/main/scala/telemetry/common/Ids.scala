package telemetry.common

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

object Ids {
  val Namespace: UUID = UUID.fromString("f5179d15-0fd7-5b5f-bf53-0852eb22d3d5")

  def uuid5(namespace: UUID, name: String): UUID = {
    val sha1 = MessageDigest.getInstance("SHA-1")
    val ns = ByteBuffer.allocate(16)
    ns.putLong(namespace.getMostSignificantBits).putLong(namespace.getLeastSignificantBits)
    sha1.update(ns.array())
    sha1.update(name.getBytes(StandardCharsets.UTF_8))
    val h = sha1.digest()
    h(6) = ((h(6) & 0x0f) | 0x50).toByte
    h(8) = ((h(8) & 0x3f) | 0x80).toByte
    val bb = ByteBuffer.wrap(h, 0, 16)
    new UUID(bb.getLong, bb.getLong)
  }

  def uuid5(name: String): String = uuid5(Namespace, name).toString

  def sessionId(vin: String, chargeStartEventId: String): String =
    uuid5(vin + "|" + chargeStartEventId.toLowerCase)

  def alertId(sourceEventId: String): String = uuid5(sourceEventId.toLowerCase)
}
