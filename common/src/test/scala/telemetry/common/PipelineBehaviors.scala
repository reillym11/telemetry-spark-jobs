package telemetry.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.funsuite.AnyFunSuite

trait PipelineBehaviors { this: AnyFunSuite =>

  /**
   * Runs the vendored sample of `inputTopic` through `transform`, checks every row against the schema of
   * `outputTopic`, and checks the result is the same for 1 batch and for 3 and 7 consecutive batches.
   * `settle` reduces raw output to what consumers see (default: the rows, sorted).
   */
  def pipeline(inputTopic: String, outputTopic: String, mode: OutputMode, expectedRows: Int)
              (transform: DataFrame => DataFrame)
              (settle: Seq[String] => Seq[String] = _.sorted): Unit = {
    val lines = Contracts.sample(inputTopic)
    lazy val once = SharedSpark.runStream(lines, Schemas.byTopic(inputTopic), 1, mode)(transform)

    test(s"$inputTopic sample -> $outputTopic validates against the schema") {
      assert(settle(once).size == expectedRows)
      once.foreach { json =>
        val errs = Contracts.errors(outputTopic, json)
        assert(errs.isEmpty, s"$json: $errs")
      }
    }

    Seq(3, 7).foreach { n =>
      test(s"$inputTopic sample -> $outputTopic is the same in $n micro-batches") {
        val split = SharedSpark.runStream(lines, Schemas.byTopic(inputTopic), n, mode)(transform)
        assert(settle(split) == settle(once))
      }
    }
  }
}
