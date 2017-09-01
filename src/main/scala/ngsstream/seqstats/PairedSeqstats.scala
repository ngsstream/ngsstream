package ngsstream.seqstats

import ngsstream.utils.FastqPair
import org.apache.spark.rdd.RDD

case class PairedSeqstats(r1: Seqstats = Seqstats(), r2: Seqstats = Seqstats()) {
  def add(pair: FastqPair): PairedSeqstats = {
    this.r1.add(pair.r1)
    this.r2.add(pair.r2)
    this
  }

  def +=(other: PairedSeqstats): PairedSeqstats = {
    this.r1 += other.r1
    this.r2 += other.r2
    this
  }
}

object PairedSeqstats {
  def fromRdd(data: RDD[FastqPair]): RDD[PairedSeqstats] = {
    data.mapPartitions { it =>
      Iterator(it.foldLeft(PairedSeqstats())((a,b) => a.add(b)))
    }
  }

  def reduce(data: RDD[PairedSeqstats]): RDD[PairedSeqstats] = {
    data.repartition(1).mapPartitions { it =>
      Iterator(it.reduce(_ += _))
    }
  }

}
