package telemetry.common

import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.networknt.schema._
import com.networknt.schema.resource.SchemaMappers

object Contracts {
  val dir: Path = Paths.get(sys.props.getOrElse("contracts.dir", "contracts")).toAbsolutePath
  private val BaseUri = "https://contracts.telemetry.example/"
  private val mapper = new ObjectMapper()

  private val factory = JsonSchemaFactory.getInstance(
    SpecVersion.VersionFlag.V202012,
    (b: JsonSchemaFactory.Builder) => {
      b.schemaMappers((m: SchemaMappers.Builder) => { m.mapPrefix(BaseUri, dir.toUri.toString); () })
      ()
    })
  private val config = SchemaValidatorsConfig.builder().formatAssertionsEnabled(true).build()

  def schemaJson(topic: String): JsonNode =
    mapper.readTree(dir.resolve(s"topics/$topic.schema.json").toFile)

  def commonJson: JsonNode = mapper.readTree(dir.resolve("common.schema.json").toFile)

  def sample(topic: String): Seq[String] =
    Files.readAllLines(dir.resolve(s"fixtures/sample/$topic.jsonl")).asScala.toSeq

  def manifest: JsonNode = mapper.readTree(dir.resolve("fixtures/sample/MANIFEST.json").toFile)

  def validator(topic: String): JsonSchema =
    factory.getSchema(SchemaLocation.of(s"${BaseUri}topics/$topic.schema.json"), config)

  /** Schema errors plus decimal-place errors (x-decimals / x-precision) for one JSON record. */
  def errors(topic: String, json: String): Seq[String] = {
    val schemaErrors = validator(topic).validate(json, InputFormat.JSON).asScala.map(_.getMessage).toSeq
    val props = schemaJson(topic).get("properties")
    val record = mapper.readTree(json)
    val decimalErrors = props.fieldNames().asScala.toSeq.flatMap { f =>
      val p = props.get(f)
      val limit = Option(p.get("x-decimals")).orElse(Option(p.get("x-precision"))).map(_.asInt)
      val v = record.get(f)
      limit.filter(_ => v != null && v.isNumber).flatMap { d =>
        val text = v.decimalValue().stripTrailingZeros()
        if (math.max(0, text.scale) > d) Some(s"$f=${v.asText} has more than $d decimals") else None
      }
    }
    schemaErrors ++ decimalErrors
  }
}
