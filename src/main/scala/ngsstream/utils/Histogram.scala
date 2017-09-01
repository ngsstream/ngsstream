package ngsstream.utils

import java.io.{File, PrintWriter}

class Histogram[T](_counts: Map[T, Long] = Map[T, Long]())(implicit ord: Numeric[T])
  extends Counts[T](_counts) {
  def aggregateStats: Map[String, Any] = {
    val values = this.counts.keys.toList
    val counts = this.counts.values.toList
    require(values.size == counts.size)
    if (values.nonEmpty) {
      val modal = values(counts.indexOf(counts.max))
      val totalCounts = counts.sum
      val mean: Double = values.zip(counts).map(x => ord.toDouble(x._1) * x._2).sum / totalCounts
      val median = values(
        values
          .zip(counts)
          .zipWithIndex
          .sortBy(_._1._1)
          .foldLeft((0L, 0)) {
            case (a, b) =>
              val total = a._1 + b._1._2
              if (total >= totalCounts / 2) (total, a._2)
              else (total, b._2)
          }
          ._2)
      Map("min" -> values.min,
        "max" -> values.max,
        "median" -> median,
        "mean" -> mean,
        "modal" -> modal)
    } else Map()
  }

  /** Write histogram to a tsv/count file */
  def writeAggregateToTsv(file: File): Unit = {
    val writer = new PrintWriter(file)
    aggregateStats.foreach(x => writer.println(x._1 + "\t" + x._2))
    writer.close()
  }
}
