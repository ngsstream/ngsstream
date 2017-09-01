package ngsstream.utils

import htsjdk.samtools.fastq.FastqRecord

case class FastqPair(r1: FastqRecord, r2: FastqRecord) {
  override def toString: String = r1.toString + "\n" + r2.toString
}

object FastqPair {
  def fromLines(lines: Seq[String]): FastqPair = {
    require(lines.size == 8)
    val r1 = new FastqRecord(lines(0).stripPrefix("@"),
                             lines(1),
                             lines(2).stripPrefix("+"),
                             lines(3))
    val r2 = new FastqRecord(lines(4).stripPrefix("@"),
                             lines(5),
                             lines(6).stripPrefix("+"),
                             lines(7))
    FastqPair(r1, r2)
  }
}
