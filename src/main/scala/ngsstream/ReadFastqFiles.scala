package ngsstream

import java.io.{File, PrintWriter}

import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class ReadFastqFiles(r1: File,
                     r2: File,
                     tempDir: File,
                     numberChunks: Int = 10,
                     minSize: Int = 50000,
                     groupSize: Int = 50)(implicit sc: SparkContext) extends Iterator[RDD[String]] with AutoCloseable {
  private val readerR1 = new FastqReader(r1)
  private val readerR2 = new FastqReader(r2)
  private val it = readerR1.iterator().zip(readerR2.iterator()).map(x => FastqPair(x._1, x._2)).grouped(groupSize)

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

    val tempFile = new File(tempDir, s"ngsstream.$outputChunkId.fq")
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
    readerR2.close()
    for (i <- 0 until numberChunks) {
      workingChunks(i) = Seq()
      completedChunks(i) = Seq()
    }
  }
}
