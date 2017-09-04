package ngsstream.utils

import htsjdk.samtools.SAMRecord

case class SamRecordPair(r1: SAMRecord,
                         r2: SAMRecord,
                         secondary: List[SAMRecord] = Nil) {
  def isMapped: Boolean = !r1.getReadUnmappedFlag || !r2.getReadUnmappedFlag
  def isSingleton: Boolean =
    isMapped && (r1.getReadUnmappedFlag || r2.getReadUnmappedFlag)
  def firstOnReference: Option[SAMRecord] =
    if (crossContig) None
    else Some(if (r1.getAlignmentStart <= r2.getAlignmentStart) r1 else r2)
  def lastOnReference: Option[SAMRecord] = firstOnReference.collect {
    case r if r == r1 => r2
    case r if r == r2 => r1
  }
  def crossContig: Boolean = {
    val c = contigs
    !isSingleton && c._1 != c._2
  }
  def contigs: (Option[String], Option[String]) =
    (Option(r1.getContig), Option(r2.getContig))

  def orientation: PairOrientation.Value = {
    (firstOnReference, lastOnReference) match {
      case (Some(first), Some(last)) =>
        val firstStrand = !first.getReadNegativeStrandFlag
        val lastStrand = !last.getReadNegativeStrandFlag
        (firstStrand, lastStrand) match {
          case (true, true) => PairOrientation.FF
          case (false, false) => PairOrientation.RR
          case (true, false) => PairOrientation.FR
          case (false, true) => PairOrientation.RF
        }
      case _ => PairOrientation.NA
    }
  }
}

object SamRecordPair {
  def fromList(records: List[SAMRecord]): SamRecordPair = {
    require(records.map(_.getReadName).distinct.size == 1,
            "Not all records have the same name")
    val secondary = records.filter(_.isSecondaryOrSupplementary)
    val r1 = records.find(r =>
      !r.isSecondaryOrSupplementary && (r.getFirstOfPairFlag || !r.getReadPairedFlag))
    val r2 =
      records.find(r => !r.isSecondaryOrSupplementary && r.getSecondOfPairFlag)
    require(r1.isDefined, "R1 is missing in the sam records")
    require(r2.isDefined, "R2 is missing in the sam records")
    require((secondary ::: r1.toList ::: r2.toList).size == records.size)
    SamRecordPair(r1.get, r2.get, secondary)
  }
}
