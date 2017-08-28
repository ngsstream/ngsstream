package ngsstream

import htsjdk.samtools.fastq.FastqRecord

case class FastqPair(r1: FastqRecord, r2: Option[FastqRecord]) {
  override def toString: String = {
    r1.toString + r2.map("\n" + _.toString).getOrElse("")
  }
}
