package ngsstream

import java.io.{ByteArrayInputStream, File, InputStream, SequenceInputStream}

import htsjdk.samtools._
import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import ngsstream.utils.SamRecordPair
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.spark
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.InterleavedFASTQInFormatter
import org.bdgenomics.adam.rdd.read.{AlignmentRecordRDD, AnySAMOutFormatter, SAMInFormatter}

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends ToolCommand[Args] {
  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    val sc = spark.loadSparkContext("ngstream", cmdArgs.sparkMaster, cmdArgs.sparkConfigValues)
    println(s"Context is up, see ${sc.uiWebUrl.getOrElse("")}")

    val rawData: AlignmentRecordRDD = sc.loadFastq(cmdArgs.r1.getAbsolutePath, Some(cmdArgs.r2.getAbsolutePath))
    val fragments = rawData.toFragments

    implicit val tFormatter = InterleavedFASTQInFormatter(fragments)
    implicit val uFormatter: AnySAMOutFormatter = new AnySAMOutFormatter
    val alignment: AlignmentRecordRDD = fragments.pipe(s"bwa mem -p ${cmdArgs.reference.getAbsolutePath} -")
    val outputBam = new File(cmdArgs.outputDir, "output.bam")
    alignment.saveAsSam(outputBam.getAbsolutePath)

    Thread.sleep(1000000)
    sc.stop()
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

  def readFastqFile(fastqFile: File): Iterator[FastqRecord] = {
    new FastqReader(fastqFile).iterator()
  }

  def samStringToSamRecordPair(x: Iterator[String]): Iterator[SamRecordPair] = {
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

  override def argsParser: AbstractOptParser[Args] = new Args.ArgsParser(this)

  override def emptyArgs: Args = Args()

  override def descriptionText: String = ""

  override def manualText: String = ""

  override def exampleText: String = ""
}
