package ngsstream

import java.io.File

import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class ReadFastqFiles(r1: File, r2: Option[File], numberChunks: Int = 10, minSize: Int = 50000, groupSize: Int = 50)(implicit sc: SparkContext) extends Iterator[RDD[FastqPair]] {
  private val readerR1 = new FastqReader(r1)
  private val readerR2 = r2.map(new FastqReader(_))
  private val it = readerR2.map(itR2 => readerR1.iterator().zip(itR2.iterator().map(Option(_))))
    .getOrElse(readerR1.iterator().map((_, None))).grouped(groupSize)

  private val workingChunks: Array[Seq[(FastqRecord, Option[FastqRecord])]] = Array.fill(numberChunks)(Seq())
  private val completedChunks: Array[Seq[(FastqRecord, Option[FastqRecord])]] = Array.fill(numberChunks)(Seq())
  //private val outputQueue: mutable.Queue[Seq[(FastqRecord, Option[FastqRecord])]] = mutable.Queue()

  def hasNext: Boolean = it.hasNext || workingChunks.exists(_.nonEmpty) || completedChunks.exists(_.nonEmpty)

  private var currentChunkId = 0

  def next(): RDD[FastqPair] = {
    val output: ListBuffer[(FastqRecord, Option[FastqRecord])] = ListBuffer()
    while (it.hasNext && output.isEmpty) {
      val list = it.next()
      workingChunks(currentChunkId) = workingChunks(currentChunkId) ++ list
      if (workingChunks(currentChunkId).size >= minSize) {
        if (completedChunks(currentChunkId).size >= minSize) {
          output ++= completedChunks(currentChunkId)
          completedChunks(currentChunkId) = workingChunks(currentChunkId)
          workingChunks(currentChunkId) = Seq()
        } else {
          completedChunks(currentChunkId) = completedChunks(currentChunkId) ++ workingChunks(currentChunkId)
          workingChunks(currentChunkId) = Seq()
        }
      }
      currentChunkId += 1
      if (currentChunkId >= numberChunks) currentChunkId = 0
    }

    if (output.isEmpty) {
      (0 until numberChunks).find(i => workingChunks(i).nonEmpty || completedChunks(i).nonEmpty) match {
        case Some(i) =>
          output ++= (completedChunks(i) ++ workingChunks(i))
          completedChunks(i) = Seq()
          workingChunks(i) = Seq()
        case _ => throw new IllegalStateException("No records left")
      }
    }
    println("Chunk is read")

    sc.parallelize(output, 1).setName("Read fastq file")
      .map(x => FastqPair(x._1, x._2)).setName("convert to FastqPair").persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  }
}
