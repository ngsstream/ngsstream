package ngsstream

import java.io.File
import java.net.URLClassLoader

import htsjdk.samtools._
import htsjdk.samtools.reference.FastaSequenceFile
import ngsstream.seqstats.PairedSeqstats
import ngsstream.utils.SamRecordPair
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object Main {
  def main(args: Array[String]): Unit = {
    val argsParser = new ArgsParser
    val cmdArgs = argsParser
      .parse(args, Args())
      .getOrElse(throw new IllegalArgumentException)

    val dict = getDictFromFasta(cmdArgs.reference)

    val jars = ClassLoader.getSystemClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .map(_.getFile)
    val conf = cmdArgs.sparkConfigValues.foldLeft(
      new SparkConf()
        .setExecutorEnv(sys.env.toArray)
        .setAppName("ngsstream")
        .setMaster(cmdArgs.sparkMaster.getOrElse("local[1]"))
        .setJars(jars))((a, b) => a.set(b._1, b._2))

    implicit val sc: SparkContext = new SparkContext(conf)
    println(s"Context is up, see ${sc.uiWebUrl.getOrElse("")}")
    val reader = new ReadFastqFiles(cmdArgs.r1, cmdArgs.r2, cmdArgs.tempDir)
    val rdds = Await.result(
      Future.sequence(reader.map(_.map(PairedProcessChunk(_, cmdArgs.reference))).toList),
      Duration.Inf
    )
    reader.close()

    val total =
      sc.union(rdds.map(_.samRecords)).persist(StorageLevel.MEMORY_ONLY_SER)
    val mappedRdd = total.filter(_.isMapped)
    val unmappedRdd = total.filter(!_.isMapped)
    val writeBamFuture = Future(
      writeToBam(mappedRdd,
                 unmappedRdd,
                 new File(cmdArgs.outputDir, "output.bam"),
                 dict,
                 cmdArgs.tempDir))
    val rawSeqStats = PairedSeqstats.reduce(sc.union(rdds.map(_.rawSeqStats))).reduce(_ += _)
    val qcSeqStats = PairedSeqstats.reduce(sc.union(rdds.map(_.qcSeqStats))).reduce(_ += _)
    println(rawSeqStats)
    println(qcSeqStats)
    val mapped = mappedRdd.countAsync()
    val unmapped = unmappedRdd.countAsync()

    println(s"mapped: ${Await.result(mapped, Duration.Inf)}")
    println(s"unmapped: ${Await.result(unmapped, Duration.Inf)}")
    Await.result(writeBamFuture, Duration.Inf)

    sc.stop()
  }

  def getDictFromFasta(fastaFile: File): SAMSequenceDictionary = {
    val referenceFile = new FastaSequenceFile(fastaFile, true)
    val dict = referenceFile.getSequenceDictionary
    referenceFile.close()
    dict
  }

  def writeToBam(mapped: RDD[SamRecordPair],
                 unmapped: RDD[SamRecordPair],
                 outputFile: File,
                 dict: SAMSequenceDictionary,
                 tempDir: File): Unit = {
    val sorted = mapped
      .flatMap(r => r.r1 :: r.r2 :: r.secondary)
      .sortBy { r =>
        val ir = r.getReferenceIndex
        val i = if (ir == -1) r.getMateReferenceIndex else ir
        (i, r.getAlignmentStart)
      }
      .persist(StorageLevel.MEMORY_ONLY_SER)

    val mappedWrite = sorted
      .mapPartitionsWithIndex {
        case (i, it) =>
          val outputFile = new File(tempDir, s"$i.bam")
          writeToBam(it, dict, outputFile)
          Iterator(i -> outputFile)
      }
      .collectAsync()

    val unmappedWrite = unmapped
      .repartition(1)
      .mapPartitions { it =>
        val outputFile = new File(tempDir, s"unmapped.bam")
        writeToBam(it.flatMap(r => r.r1 :: r.r2 :: r.secondary),
                   dict,
                   outputFile)
        Iterator(-1 -> outputFile)
      }
      .collectAsync()

    val bamFiles = Await.result(mappedWrite, Duration.Inf) ++ Await.result(
      unmappedWrite,
      Duration.Inf)
    sorted.context.parallelize(bamFiles, 1).foreachPartition { i =>
      println("Writing complete bam file")
      val readers = i.map(x => x._1 -> SamReaderFactory.make().open(x._2))
      val it = readers.map(_._2.toIterator).reduce(_ ++ _)
      writeToBam(it, dict, outputFile)
      println("Removing temp bam files")
      bamFiles.foreach { x =>
        x._2.delete()
        new File(x._2.getAbsolutePath.stripSuffix(".bam") + ".bai").delete()
        new File(x._2.getAbsolutePath + ".md5").delete()
      }
    }
  }

  def createSamWriter(dict: SAMSequenceDictionary,
                      outputFile: File): SAMFileWriter = {
    val header = new SAMFileHeader
    header.setSequenceDictionary(dict)
    header.setSortOrder(SAMFileHeader.SortOrder.coordinate)

    SAMFileWriterFactory.setDefaultCreateIndexWhileWriting(true)
    SAMFileWriterFactory.setDefaultCreateMd5File(true)

    new SAMFileWriterFactory()
      .setUseAsyncIo(true)
      .setCreateIndex(true)
      .setCreateMd5File(true)
      .makeBAMWriter(header, true, outputFile)
  }

  def writeToBam(it: Iterator[SAMRecord],
                 dict: SAMSequenceDictionary,
                 outputFile: File): Unit = {
    val writer = createSamWriter(dict, outputFile)
    it.foreach(writer.addAlignment)
    writer.close()
  }
}
