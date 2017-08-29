package ngsstream

import java.io.File

case class Args(r1: File = null,
                r2: Option[File] = None,
                outputDir: File = null,
                reference: File = null,
                sparkMaster: Option[String] = None)

class ArgsParser extends scopt.OptionParser[Args]("ngsstream") {
  opt[File]('1', "fastqR1").required().action((x,c) => c.copy(r1 = x))
  opt[File]('2', "fastqR2").action((x,c) => c.copy(r2 = Some(x)))
  opt[File]('o', "outputDir").required().action((x,c) => c.copy(outputDir = x))
  opt[File]('R', "reference").required().action((x,c) => c.copy(reference = x))
  opt[String]("sparkMaster").action((x,c) => c.copy(sparkMaster = Some(x)))
}

