package telemetry.common

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class SchemasSpec extends AnyFunSuite {

  private val defs = Contracts.commonJson.get("$defs")

  private def resolve(p: JsonNode): (JsonNode, Option[String]) =
    Option(p.get("$ref")).map(_.asText) match {
      case Some(ref) =>
        val name = ref.split("/").last
        (defs.get(name), Some(name))
      case None => (p, None)
    }

  private def expected(topic: String): StructType = {
    val schema = Contracts.schemaJson(topic)
    val required = schema.get("required").elements().asScala.map(_.asText).toSet
    StructType(schema.get("properties").fields().asScala.toSeq.map { e =>
      val (p, defName) = resolve(e.getValue)
      val types = p.get("type") match {
        case t if t.isArray => t.elements().asScala.map(_.asText).toSeq
        case t => Seq(t.asText)
      }
      val base = types.filterNot(_ == "null") match {
        case Seq("string") if defName.contains("timestamp") => TimestampType
        case Seq("string") => StringType
        case Seq("integer") => LongType
        case Seq("number") => DoubleType
        case other => fail(s"$topic.${e.getKey}: unsupported type $other")
      }
      StructField(e.getKey, base, nullable = types.contains("null") || !required(e.getKey))
    })
  }

  Schemas.byTopic.foreach { case (topic, struct) =>
    test(s"$topic StructType matches the vendored JSON Schema") {
      assert(struct == expected(topic))
    }
  }

  test("every input topic in topics.yaml has a StructType") {
    val yaml = new String(java.nio.file.Files.readAllBytes(Contracts.dir.resolve("topics.yaml")), "UTF-8")
    val inputs = "(?m)^- name: (vehicle\\.\\S+)$".r.findAllMatchIn(yaml).map(_.group(1)).toSet
    assert(inputs == Schemas.byTopic.keySet)
  }
}
