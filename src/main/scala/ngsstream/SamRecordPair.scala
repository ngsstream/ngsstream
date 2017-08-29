package ngsstream

import htsjdk.samtools.SAMRecord

case class SamRecordPair(r1: SAMRecord, r2: Option[SAMRecord], secondary: List[SAMRecord] = Nil) {

}

object SamRecordPair {
  def fromList(records: List[SAMRecord]): SamRecordPair = {
    val secondary = records.filter(_.isSecondaryOrSupplementary)
    val r1 = records.find(r => !r.isSecondaryOrSupplementary && (r.getFirstOfPairFlag || !r.getReadPairedFlag))
    val r2 = records.find(r => !r.isSecondaryOrSupplementary && r.getSecondOfPairFlag)
    require(r1.isDefined, "R1 is missing in the sam records")
    require((secondary ::: r1.toList ::: r2.toList).size == records.size)
    SamRecordPair(r1.get, r2, secondary)
  }
}
