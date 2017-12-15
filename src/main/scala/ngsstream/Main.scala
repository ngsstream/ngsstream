package org.bdgenomics.adam.rdd

import java.io.{ByteArrayInputStream, File, InputStream, SequenceInputStream}

import htsjdk.samtools._
import htsjdk.samtools.fastq.{FastqReader, FastqRecord}
import ngsstream.Args
import ngsstream.utils.SamRecordPair
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.spark
import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}
import org.apache.hadoop.io.Text
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.bdgenomics.adam.converters.FastqRecordConverter
import org.bdgenomics.adam.io.{InterleavedFastqInputFormat, SingleFastqInputFormat}
import org.bdgenomics.adam.models.{RecordGroup, RecordGroupDictionary, SequenceDictionary}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.fragment.{FragmentRDD, InterleavedFASTQInFormatter}
import org.bdgenomics.adam.rdd.read.{AlignmentRecordRDD, AnySAMOutFormatter, SAMInFormatter}
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.misc.HadoopUtil
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Main extends ToolCommand[Args] {

  implicit class BiopetContext(sc: SparkContext) {
    def loadPairedFastqAsFragments(pathNameR1: String,
                                   pathNameR2: String,
                                   recordGroup: RecordGroup): FragmentRDD = {

      def readFastq(path: String) = {
        val job = HadoopUtil.newJob(sc)
        val conf = ContextUtil.getConfiguration(job)
        conf.setStrings("io.compression.codecs",
          classOf[BGZFCodec].getCanonicalName,
          classOf[BGZFEnhancedGzipCodec].getCanonicalName)
        val file = sc.newAPIHadoopFile(
          path,
          classOf[SingleFastqInputFormat],
          classOf[Void],
          classOf[Text],
          conf
        )
        if (Metrics.isRecording) file//.instrument()
        else file
      }
      val recordsR1 = readFastq(pathNameR1)
      val recordsR2 = readFastq(pathNameR2)

      // convert records
      val fastqRecordConverter = new FastqRecordConverter
      // Zip will fail if R1 and R2 has an different number of reads
      // Checking this explicitly like in loadPairedFastq is not required and not blocking anymore
      val rdd = recordsR1.zip(recordsR2)
        .map {
          case (r1, r2) =>
            val pairText = new Text(r1._2.toString + r2._2.toString)
            fastqRecordConverter.convertFragment((null, pairText))
        }
      FragmentRDD(rdd, SequenceDictionary.empty, RecordGroupDictionary(Seq(recordGroup)))
    }

  }

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    val sc = spark.loadSparkContext("ngstream", cmdArgs.sparkMaster, cmdArgs.sparkConfigValues)
    println(s"Context is up, see ${sc.uiWebUrl.getOrElse("")}")

    val outputBam = new File(cmdArgs.outputDir, "output.bam")

    val dict = SequenceDictionary.fromSAMSequenceDictionary(fasta.getCachedDict(cmdArgs.reference))

    val rawData = sc.loadPairedFastqAsFragments(cmdArgs.r1.getAbsolutePath,
      cmdArgs.r2.getAbsolutePath, RecordGroup("sampleName", "RgName"))

    implicit val tFormatter = InterleavedFASTQInFormatter
    implicit val uFormatter: AnySAMOutFormatter = new AnySAMOutFormatter
    val alignment: AlignmentRecordRDD = rawData.copy(rdd = rawData.rdd.repartition(10)).pipe(s"/Users/pjvanthof/bin/bwa mem -p ${cmdArgs.reference.getAbsolutePath} -")
    alignment.copy(sequences = dict, rdd = alignment.rdd.map { x =>
      x.setRecordGroupName("RgName")
      x
    }).sortReadsByReferencePosition().markDuplicates().saveAsSam(outputBam.getAbsolutePath, asSingleFile = true)

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
