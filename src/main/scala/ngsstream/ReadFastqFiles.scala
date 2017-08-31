package ngsstream

import java.io.{File, PrintWriter}

import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class ReadFastqFiles(r1: File,
                     r2: Option[File],
                     numberChunks: Int = 10,
                     minSize: Int = 50000,
                     groupSize: Int = 50,
                     tempDir: Option[File] = None)(implicit sc: SparkContext) extends Iterator[RDD[String]] with AutoCloseable {
  private val readerR1 = new FastqReader(r1)
  private val readerR2 = r2.map(new FastqReader(_))
  private val it = readerR2.map(itR2 => readerR1.iterator().zip(itR2.iterator().map(Option(_))))
    .getOrElse(readerR1.iterator().map((_, None))).grouped(groupSize).map(_.map(x => FastqPair(x._1, x._2)))

  private val workingChunks: Array[Seq[FastqPair]] = Array.fill(numberChunks)(Seq())
  private val completedChunks: Array[Seq[FastqPair]] = Array.fill(numberChunks)(Seq())
  //private val outputQueue: mutable.Queue[Seq[(FastqRecord, Option[FastqRecord])]] = mutable.Queue()

  def hasNext: Boolean = it.hasNext || workingChunks.exists(_.nonEmpty) || completedChunks.exists(_.nonEmpty)

  private var outputChunkId = 0
  private var currentChunkId = 0

  def next(): RDD[String] = {
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

    val tempFile = tempDir match {
      case Some(dir) => new File(dir, s"ngsstream.$outputChunkId.fq")
      case _ => File.createTempFile(s"ngsstream.$outputChunkId.", ".fq")
    }
    tempFile.deleteOnExit()
    val writer = new PrintWriter(tempFile)
    output.foreach(writer.println)
    writer.close()
    output.clear()
    outputChunkId += 1

    sc.textFile(tempFile.getAbsolutePath, 1).setName("Read fastq file").persist(StorageLevel.MEMORY_ONLY_SER)
  }

  def close(): Unit = {
    readerR1.close()
    readerR2.foreach(_.close())
    for (i <- 0 until numberChunks) {
      workingChunks(i) = Seq()
      completedChunks(i) = Seq()
    }
  }
}
