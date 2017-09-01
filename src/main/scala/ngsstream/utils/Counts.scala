package ngsstream.utils

import java.io.{File, PrintWriter}

import scala.collection.mutable

class Counts[T](_counts: Map[T, Long] = Map[T, Long]())(
    implicit ord: Ordering[T])
    extends Serializable {
  protected[Counts] val counts: mutable.Map[T, Long] = mutable.Map() ++ _counts

  /** Returns histogram as map */
  def countsMap: Map[T, Long] = counts.toMap

  /** Returns value if it does exist */
  def get(key: T): Option[Long] = counts.get(key)

  /** This will add an other histogram to `this` */
  def +=(other: Counts[T]): Counts[T] = {
    other.counts.foreach(x =>
      this.counts += x._1 -> (this.counts.getOrElse(x._1, 0L) + x._2))
    this
  }

  /** With this a value can be added to the histogram */
  def add(value: T): Unit = {
    counts += value -> (counts.getOrElse(value, 0L) + 1)
  }

  /** Write histogram to a tsv/count file */
  def writeHistogramToTsv(file: File): Unit = {
    val writer = new PrintWriter(file)
    writer.println("value\tcount")
    counts.keys.toList.sorted.foreach(x => writer.println(s"$x\t${counts(x)}"))
    writer.close()
  }

  def toSummaryMap: Map[String, List[Any]] = {
    val values = counts.keySet.toList.sortWith(Counts.sortAnyAny)
    Map("values" -> values, "counts" -> values.map(counts(_)))
  }

  override def equals(other: Any): Boolean = {
    other match {
      case c: Counts[T] => this.counts == c.counts
      case _ => false
    }
  }
}
object Counts {

  /** Function to sort Any values */
  def sortAnyAny(a: Any, b: Any): Boolean = {
    a match {
      case ai: Int =>
        b match {
          case bi: Int => ai < bi
          case bi: Double => ai < bi
          case _ => a.toString < b.toString
        }
      case _ => a.toString < b.toString
    }
  }
}
