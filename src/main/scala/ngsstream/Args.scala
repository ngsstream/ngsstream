package ngsstream

import java.io.File

case class Args(r1: File = null,
                r2: Option[File] = None,
                outputDir: File = null,
                reference: File = null,
                tempDir: Option[File] = None,
                sparkMaster: Option[String] = None,
                sparkConfigValues: Map[String, String] = Map(
                  "spark.memory.fraction" -> "0.1",
                  "spark.memory.storageFraction" -> "0.2",
                  "spark.driver.maxResultSize" -> "2G",
                  "spark.driver.memory" -> "2G"
                )
               )

class ArgsParser extends scopt.OptionParser[Args]("ngsstream") {
  opt[File]('1', "fastqR1").required().action((x,c) => c.copy(r1 = x))
  opt[File]('2', "fastqR2").action((x,c) => c.copy(r2 = Some(x)))
  opt[File]('o', "outputDir").required().action((x,c) => c.copy(outputDir = x))
  opt[File]('R', "reference").required().action((x,c) => c.copy(reference = x))
  opt[File]("tempDir").action((x,c) => c.copy(tempDir = Some(x)))
    .text("This should be a path that is readable to all executors")
  opt[String]("sparkMaster").action((x,c) => c.copy(sparkMaster = Some(x)))
  opt[(String, String)]("sparkConfigValue").unbounded().action( (x, c) =>
    c.copy(sparkConfigValues = c.sparkConfigValues + (x._1 -> x._2))
  ).text(s"Add values to the spark config")
}

