package ngsstream


import htsjdk.samtools._
import org.apache.spark._

import scala.collection.JavaConversions._
import java.io.{ByteArrayInputStream, File, InputStream, SequenceInputStream}
import java.net.URLClassLoader

import htsjdk.samtools.reference.FastaSequenceFile
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
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
        .setMaster(cmdArgs.sparkMaster.getOrElse("local[2]"))
        .setJars(jars))((a, b) => a.set(b._1, b._2))

    implicit val sc: SparkContext = new SparkContext(conf)
    val reader = new ReadFastqFiles(cmdArgs.r1, cmdArgs.r2, cmdArgs.tempDir)
    val rdds = reader.map(ori => ori -> {
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
        }.setName("convert to sam records")//.cache()
      rdd -> rdd.countAsync()
    }
    ).toList
    reader.close()

    val reads = rdds.map(_._1.count()).sum / 4
    println(s"reads: $reads")

    val total = sc.union(rdds.map(_._2._1)).persist(StorageLevel.MEMORY_ONLY_SER)
    val mapped = total.filter(_.isMapped)
    val sorted = mapped
      .sortBy{r =>
        val contig = r.contigs
        val pos = Option(r.r1.getAlignmentStart)
        (contig, pos)
      }.persist(StorageLevel.MEMORY_ONLY_SER)

    val unmapped = total.filter(!_.isMapped)
    val crossContigs = mapped.filter(_.crossContig)
    println(s"mapped: ${mapped.count()}")
    println(s"unmapped: ${unmapped.count()}")
    println(s"crossContigs: ${crossContigs.count()}")
    writeToBam(mapped, unmapped, new File(cmdArgs.outputDir, "output.bam"), dict, cmdArgs.tempDir)

    Thread.sleep(100000)

    sc.stop()
  }

  def getDictFromFasta(fastaFile: File): SAMSequenceDictionary = {
    val referenceFile = new FastaSequenceFile(fastaFile, true)
    val dict = referenceFile.getSequenceDictionary
    referenceFile.close()
    dict
  }

  def writeToBam(mapped: RDD[SamRecordPair], unmapped: RDD[SamRecordPair], outputFile: File, dict: SAMSequenceDictionary, tempDir: File): Unit = {
    val sorted = mapped.flatMap(r => r.r1 :: r.r2 :: r.secondary)
      .sortBy(r => (Option(r.getContig), Option(r.getAlignmentStart))).persist(StorageLevel.MEMORY_ONLY_SER)
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

    val bla = sorted.mapPartitionsWithIndex { case (i, it) =>
      val header = new SAMFileHeader
      header.setSequenceDictionary(dict)
      header.setSortOrder(SAMFileHeader.SortOrder.coordinate)
      SAMFileWriterFactory.setDefaultCreateIndexWhileWriting(true)
      SAMFileWriterFactory.setDefaultCreateMd5File(true)
      val writer = new SAMFileWriterFactory()
        .setUseAsyncIo(true)
        .setCreateIndex(true)
        .setCreateMd5File(true)
        .makeBAMWriter(header, true, new File(tempDir, s"$i.bam"))
      it.foreach(writer.addAlignment)
      writer.close()
      Iterator()
    }.count()
    unmapped.repartition(1).mapPartitions { it =>
      val writer = new SAMFileWriterFactory()
        .setUseAsyncIo(true)
        .setCreateIndex(true)
        .setCreateMd5File(true)
        .makeBAMWriter(header, true, new File(tempDir, s"unmapped.bam"))
      it.foreach(r => (r.r1 :: r.r2 :: r.secondary).foreach(writer.addAlignment))
      writer.close()
      Iterator()
    }.count()
    //sorted.toLocalIterator.foreach(writer.addAlignment)
    //sorted.unpersist()
    writer.close()
  }
}
