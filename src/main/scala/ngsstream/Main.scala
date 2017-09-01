package ngsstream


import htsjdk.samtools._
import org.apache.spark._

import scala.collection.JavaConversions._
import java.io.{ByteArrayInputStream, File, InputStream, SequenceInputStream}
import java.net.URLClassLoader

import htsjdk.samtools.reference.FastaSequenceFile
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

object Main {
  def main(args: Array[String]): Unit = {
    val argsParser = new ArgsParser
    val cmdArgs = argsParser.parse(args, Args()).getOrElse(throw new IllegalArgumentException)

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
    val reader = new ReadFastqFiles(cmdArgs.r1, cmdArgs.r2, cmdArgs.tempDir)
    val rdds = Await.result(Future.sequence(reader.map(_.map(ori => ori -> {
      val rdd = ori
        //.pipe(s"cutadapt --interleaved -").setName("cutadapt")
        .pipe(s"bwa mem -p ${cmdArgs.reference} -").setName("bwa mem")
        .mapPartitions { x =>
          val stream: InputStream = new SequenceInputStream(
            x.map(_ + "\n").map( s => new ByteArrayInputStream(s.getBytes("UTF-8")))
          )
          SamReaderFactory.make().open(SamInputResource.of(stream))
            .iterator()
            .toList
            .groupBy(_.getReadName)
            .iterator
            .map(x => SamRecordPair.fromList(x._2.toList))
        }.setName("convert to sam records").cache()
      rdd -> rdd.countAsync()
    }
    )).toList), Duration.Inf)
    reader.close()

    val reads = sc.union(rdds.map(_._1)).countAsync().map(_ / 4)
    val total = sc.union(rdds.map(_._2._1)).persist(StorageLevel.MEMORY_ONLY_SER)
    val mappedRdd = total.filter(_.isMapped)
    val singletonsRdd = total.filter(_.isSingleton)
    val unmappedRdd = total.filter(!_.isMapped)
    val crossContigsRdd = mappedRdd.filter(_.crossContig)
    val secondaryRdd = mappedRdd.flatMap(_.secondary)
    val writeBamFuture = Future(writeToBam(mappedRdd, unmappedRdd, new File(cmdArgs.outputDir, "output.bam"), dict, cmdArgs.tempDir))
    val mapped = mappedRdd.countAsync()
    val unmapped = unmappedRdd.countAsync()
    val singletons = singletonsRdd.countAsync()
    val crossContigs = crossContigsRdd.countAsync()
    val secondary = secondaryRdd.countAsync()

    println(s"mapped: ${Await.result(mapped, Duration.Inf)}")
    println(s"unmapped: ${Await.result(unmapped, Duration.Inf)}")
    println(s"singletons: ${Await.result(singletons, Duration.Inf)}")
    println(s"crossContigs: ${Await.result(crossContigs, Duration.Inf)}")
    println(s"secondary: ${Await.result(secondary, Duration.Inf)}")
    println(s"reads: ${Await.result(reads, Duration.Inf)}")
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
    val sorted = mapped.flatMap(r => r.r1 :: r.r2 :: r.secondary)
      .sortBy { r =>
        val ir = r.getReferenceIndex
        val i = if (ir == -1) r.getMateReferenceIndex else ir
        (i, r.getAlignmentStart)
      }
      .persist(StorageLevel.MEMORY_ONLY_SER)

    val mappedWrite = Future {
      sorted.mapPartitionsWithIndex { case (i, it) =>
        writeToBam(it, dict, new File(tempDir, s"$i.bam"))
        Iterator()
      }.count()
    }

    val unmappedWrite = Future {
      unmapped.repartition(1).mapPartitions { it =>
        writeToBam(it.flatMap(r => r.r1 :: r.r2 :: r.secondary), dict, new File(tempDir, s"unmapped.bam"))
        Iterator()
      }.count()
    }

    Await.result(mappedWrite, Duration.Inf)
    Await.result(unmappedWrite, Duration.Inf)
  }

  def writeToBam(it: Iterator[SAMRecord], dict: SAMSequenceDictionary, outputFile: File): Unit = {
    val header = new SAMFileHeader
    header.setSequenceDictionary(dict)
    header.setSortOrder(SAMFileHeader.SortOrder.coordinate)

    SAMFileWriterFactory.setDefaultCreateIndexWhileWriting(true)
    SAMFileWriterFactory.setDefaultCreateMd5File(true)

    val writer = new SAMFileWriterFactory()
      .setUseAsyncIo(true)
      .setCreateIndex(true)
      .setCreateMd5File(true)
      .makeBAMWriter(header, true, outputFile)
    it.foreach(writer.addAlignment)
    writer.close()
  }
}
