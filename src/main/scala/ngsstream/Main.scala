package ngsstream

import org.apache.spark._

object Main {
  def main(args: Array[String]): Unit = {
    val argsParser = new ArgsParser
    val cmdArgs = argsParser.parse(args, Args()).getOrElse(throw new IllegalArgumentException)

    val conf = new SparkConf()
      .setAppName("ngsstream")
      .setMaster(cmdArgs.sparkMaster.getOrElse("local[*]"))
    implicit val sc: SparkContext = new SparkContext(conf)

    val it = new ReadFastqFiles(cmdArgs.r1, cmdArgs.r2)

    val rdds = it.map(_.pipe("sickle pe -t sanger -c /dev/stdin -M /dev/stdout")).toList

    val total = sc.union(rdds.map(_.map(_.toString())))
    println("count: " + total.count())
    rdds.head.foreach(println)

    Thread.sleep(100000)

    sc.stop()
  }
}
