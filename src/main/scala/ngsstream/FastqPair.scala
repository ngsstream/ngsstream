package ngsstream

import htsjdk.samtools.fastq.FastqRecord

case class FastqPair(r1: FastqRecord, r2: FastqRecord) {
  override def toString: String = r1.toString + "\n" + r2.toString
}
