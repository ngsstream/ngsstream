package ngsstream.seqstats

import htsjdk.samtools.fastq.FastqRecord
import ngsstream.utils.Histogram
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

case class Seqstats(positions: ArrayBuffer[Position] = ArrayBuffer(),
                    readLength: Histogram[Int] = new Histogram[Int]()) {

  def add(record: FastqRecord): Seqstats = {
    val length = record.getReadLength
    readLength.add(length)
    if (positions.size < length)
      positions.append(Array.fill(length - positions.size)(Position()): _*)
    record.getBaseQualities.zipWithIndex.foreach(x =>
      positions(x._2).qual.add(x._1))
    record.getReadBases.zipWithIndex.foreach(x =>
      positions(x._2).nuc.add(x._1))
    this
  }

  def +=(other: Seqstats): Seqstats = {
    this.readLength += other.readLength
    if (this.positions.size > other.positions.size)
      other.positions.append(
        Array.fill(this.positions.size - other.positions.size)(Position()): _*)
    if (this.positions.size < other.positions.size)
      this.positions.append(
        Array.fill(other.positions.size - this.positions.size)(Position()): _*)
    this.positions.zip(other.positions).foreach(x => x._1 += x._2)
    this
  }
}

object Seqstats {

  def fromRdd(data: RDD[FastqRecord]): RDD[Seqstats] = {
    data.mapPartitions { it =>
      Iterator(it.foldLeft(Seqstats())((a, b) => a.add(b)))
    }
  }

  def reduce(data: RDD[Seqstats]): RDD[Seqstats] = {
    data.repartition(1).mapPartitions { it =>
      Iterator(it.reduce(_ += _))
    }
  }
}
