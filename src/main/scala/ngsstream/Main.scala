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

    val paired = cmdArgs.r2.isDefined
    val dict = getDictFromFasta(cmdArgs.reference)

    val jars = ClassLoader.getSystemClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .map(_.getFile)
    val conf = new SparkConf()
      .setExecutorEnv(sys.env.toArray)
      .setAppName("ngsstream")
      .setMaster(cmdArgs.sparkMaster.getOrElse("local[2]"))
      .setJars(jars)
    implicit val sc: SparkContext = new SparkContext(conf)
sc.binaryFiles()
    val rdds = new ReadFastqFiles(cmdArgs.r1, cmdArgs.r2).map(ori => ori -> {
      val rdd = ori
        //.pipe(s"cutadapt ${if (paired) "--interleaved" else ""} -").setName("cutadapt")
        .pipe(s"bwa mem ${if (paired) "-p" else ""} ${cmdArgs.reference} -").setName("bwa mem")
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
    )

    val total = sc.union(rdds.map(_._2._1).toList)
      .sortBy(r => (r.r1.getContig, r.r1.getAlignmentStart)).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//    println(s"${total.count()} fragments found")
    rdds.foreach(_._1.unpersist(false))
    rdds.foreach(_._2._1.unpersist(false))

    //total.foreach(r => println(r.r1.getContig + "\t" + r.r1.getAlignmentStart))
    //println("contigs: " + total.keys.collect().mkString(", "))

    writeToBam(total, new File(cmdArgs.outputDir, "output.bam"), dict)

    Thread.sleep(100000)

    sc.stop()
  }

  def getDictFromFasta(fastaFile: File): SAMSequenceDictionary = {
    val referenceFile = new FastaSequenceFile(fastaFile, true)
    val dict = referenceFile.getSequenceDictionary
    referenceFile.close()
    dict
  }

  def writeToBam(records: RDD[SamRecordPair], outputFile: File, dict: SAMSequenceDictionary): Unit = {
    val sorted = records.flatMap(r => r.r1 :: r.r2.toList ::: r.secondary).sortBy(r => (r.getContig, r.getAlignmentStart))
    val header = new SAMFileHeader
    header.setSequenceDictionary(dict)
    header.setSortOrder(SAMFileHeader.SortOrder.coordinate)
    SAMFileWriterFactory.setDefaultCreateIndexWhileWriting(true)
    SAMFileWriterFactory.setDefaultCreateMd5File(true)
    val writer = new SAMFileWriterFactory()
      .setUseAsyncIo(true)
      .setCreateIndex(true)
      .makeBAMWriter(header, true, outputFile)
    sorted.toLocalIterator.foreach(r => writer.addAlignment(r))
    writer.close()
  }
}
