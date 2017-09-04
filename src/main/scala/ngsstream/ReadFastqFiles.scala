package ngsstream

import java.io.{File, FileOutputStream, PrintWriter}
import java.util.zip.GZIPOutputStream

import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import ngsstream.utils.FastqPair
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ReadFastqFiles(r1: File,
                     r2: File,
                     tempDir: File,
                     numberChunks: Int = 5,
                     minSize: Int = 100000,
                     groupSize: Int = 50)(implicit sc: SparkContext)
    extends Iterator[Future[RDD[String]]]
    with AutoCloseable {
  private val readerR1 = new FastqReader(r1)
  private val readerR2 = new FastqReader(r2)
  private val it = readerR1
    .iterator()
    .zip(readerR2.iterator())
    .map(x => FastqPair(x._1, x._2))
    .grouped(groupSize)

  private val workingChunks: Array[Seq[FastqPair]] =
    Array.fill(numberChunks)(Seq())
  private val completedChunks: Array[Seq[FastqPair]] =
    Array.fill(numberChunks)(Seq())
  //private val outputQueue: mutable.Queue[Seq[(FastqRecord, Option[FastqRecord])]] = mutable.Queue()

  def hasNext: Boolean =
    it.hasNext || workingChunks.exists(_.nonEmpty) || completedChunks.exists(
      _.nonEmpty)

  private var outputChunkId = 0
  private var currentChunkId = 0

  def next(): Future[RDD[String]] = {
    val output: ListBuffer[FastqPair] = ListBuffer()
    while (it.hasNext && output.isEmpty) {
      val list = it.next()
      workingChunks(currentChunkId) = workingChunks(currentChunkId) ++ list
      if (workingChunks(currentChunkId).size >= minSize) {
        if (completedChunks(currentChunkId).size >= minSize) {
          output ++= completedChunks(currentChunkId)
          completedChunks(currentChunkId) = workingChunks(currentChunkId)
          workingChunks(currentChunkId) = Seq()
        } else {
          completedChunks(currentChunkId) = completedChunks(currentChunkId) ++ workingChunks(
            currentChunkId)
          workingChunks(currentChunkId) = Seq()
        }
      }
      currentChunkId += 1
      if (currentChunkId >= numberChunks) currentChunkId = 0
    }

    if (output.isEmpty) {
      (0 until numberChunks).find(i =>
        workingChunks(i).nonEmpty || completedChunks(i).nonEmpty) match {
        case Some(i) =>
          output ++= (completedChunks(i) ++ workingChunks(i))
          completedChunks(i) = Seq()
          workingChunks(i) = Seq()
        case _ => throw new IllegalStateException("No records left")
      }
    }
    println(s"Chunk $outputChunkId is read")

    val id = outputChunkId
    outputChunkId += 1
    Future {
      val tempFile = new File(tempDir, s"ngsstream.$id.fq.gz")
      tempFile.deleteOnExit()
      val fos = new FileOutputStream(tempFile)
      val gzos = new GZIPOutputStream(fos)
      val writer = new PrintWriter(gzos)
      output.foreach(writer.println)
      writer.close()
      gzos.close()
      fos.close()
      output.clear()
      sc.textFile(tempFile.getAbsolutePath, 1)
        .setName("Read fastq file")
        .persist(StorageLevel.MEMORY_ONLY_SER)
    }
  }

  def close(): Unit = {
    readerR1.close()
    readerR2.close()
    for (i <- 0 until numberChunks) {
      workingChunks(i) = Seq()
      completedChunks(i) = Seq()
    }
  }
}
