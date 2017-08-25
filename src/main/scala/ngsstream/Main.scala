package ngsstream


import org.apache.spark._
import org.apache.spark.streaming._

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  def main(args: Array[String]): Unit = {
    val argsParser = new ArgsParser
    val cmdArgs = argsParser.parse(args, Args()).getOrElse(throw new IllegalArgumentException)

    val conf = new SparkConf().setAppName("ngsstream").setMaster(s"local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))


    val lines = ssc.socketTextStream("sasc-peter.lumcnet.prod.intern", 6666)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.foreachRDD { x =>
      x.foreach(println)
    }

    ssc.start()

    Thread.sleep(5000)

    val future = Future {
      import java.net._
      import java.io._
      import scala.io._


      val s = new Socket(InetAddress.getByName("sasc-peter.lumcnet.prod.intern"), 6666)
      lazy val in = new BufferedSource(s.getInputStream()).getLines()
      val out = new PrintStream(s.getOutputStream())

      out.println("Hello, world")
      out.flush()
      s.close()
    }
    Await.result(future, scala.concurrent.duration.Duration.Inf)
    lines.stop()





    Thread.sleep(10000)

    Thread.sleep(100000)

    sc.stop()
  }
}
