package ngsstream

import java.io.{ByteArrayInputStream, File, InputStream, SequenceInputStream}

import htsjdk.samtools.{SamInputResource, SamReaderFactory}
import ngsstream.bamstats.PairedFlagstats
import ngsstream.seqstats.PairedSeqstats
import ngsstream.utils.{FastqPair, SamRecordPair}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class PairedProcessChunk(rawData: RDD[String], reference: File) {
  lazy val rawFastqPair: RDD[FastqPair] =
    PairedProcessChunk.stringToFastqPair(rawData)
  lazy val rawSeqStats: RDD[PairedSeqstats] =
    PairedSeqstats.fromRdd(rawFastqPair).setName("Raw Seqstats").persist()
  rawSeqStats.countAsync()
  protected val clipped
    : RDD[String] = rawData //.pipe(s"cutadapt --interleaved -").setName("cutadapt")
  lazy val qcFastqPair: RDD[FastqPair] =
    PairedProcessChunk.stringToFastqPair(clipped)
  lazy val qcSeqStats: RDD[PairedSeqstats] =
    PairedSeqstats.fromRdd(qcFastqPair).setName("QC Seqstats").persist()
  qcSeqStats.countAsync()
  protected val mapped: RDD[String] =
    clipped.pipe(s"bwa mem -p $reference -").setName("bwa mem")
  val samRecords: RDD[SamRecordPair] = {
    mapped
      .mapPartitions { x =>
        val stream: InputStream = new SequenceInputStream(
          x.map(_ + "\n")
            .map(s => new ByteArrayInputStream(s.getBytes("UTF-8")))
        )
        SamReaderFactory
          .make()
          .open(SamInputResource.of(stream))
          .iterator()
          .toList
          .groupBy(_.getReadName)
          .iterator
          .map(x => SamRecordPair.fromList(x._2.toList))
      }
      .setName("convert to sam records")
      .cache()
  }
  private val _samPairsCount = samRecords.countAsync()
  val contigGrouped: RDD[((Option[String], Option[String]), Iterable[SamRecordPair])] = samRecords.groupBy(_.contigs)
  def samPairsCount: Long = Await.result(_samPairsCount, Duration.Inf)

  val flagstats = contigGrouped.map(x =>   x._1 -> PairedFlagstats.generate(x._2))
  flagstats.countAsync()
}

object PairedProcessChunk {
  def apply(rawData: RDD[String], reference: File): PairedProcessChunk =
    new PairedProcessChunk(rawData, reference)

  def stringToFastqPair(data: RDD[String]): RDD[FastqPair] = {
    data
      .mapPartitions(it => it.grouped(8).map(FastqPair.fromLines))
      .setName("To FastqPair")
  }
}
