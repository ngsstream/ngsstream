package ngsstream.bamstats

import htsjdk.samtools.SAMRecord

case class Flagstats(counts: Map[Flagstats.Flags.Value, Long] = Flagstats.Flags.values.map(_ -> 0L).toMap) {
  def +(other: Flagstats): Flagstats = {
    Flagstats(Flagstats.Flags.values
      .map(flag => flag -> (other.counts.getOrElse(flag, 0L) + this.counts.getOrElse(flag, 0L))).toMap)
  }
}

object Flagstats {
  object Flags extends Enumeration {
    val All, Mapped, Duplicates, FirstOfPair, SecondOfPair, ReadStrand, Secondary = Value
  }

  def generate(records: Iterable[SAMRecord]): Flagstats = {
    Flagstats(Map(
      Flags.All -> records.size,
      Flags.Mapped -> records.count(!_.getReadUnmappedFlag),
      Flags.Duplicates -> records.count(_.getDuplicateReadFlag),
      Flags.FirstOfPair -> records.count(_.getFirstOfPairFlag),
      Flags.SecondOfPair -> records.count(_.getSecondOfPairFlag),
      Flags.ReadStrand -> records.count(!_.getReadNegativeStrandFlag),
      Flags.Secondary -> records.count(_.isSecondaryOrSupplementary)
    ))
  }
}