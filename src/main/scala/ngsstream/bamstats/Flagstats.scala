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

  def generate(records: Iterator[SAMRecord]): Flagstats = {
    val list = records.toList
    Flagstats(Map(
      Flags.All -> list.size,
      Flags.Mapped -> list.count(!_.getReadUnmappedFlag),
      Flags.Duplicates -> list.count(_.getDuplicateReadFlag),
      Flags.FirstOfPair -> list.count(_.getFirstOfPairFlag),
      Flags.SecondOfPair -> list.count(_.getSecondOfPairFlag),
      Flags.ReadStrand -> list.count(!_.getReadNegativeStrandFlag),
      Flags.Secondary -> list.count(_.isSecondaryOrSupplementary)
    ))
  }
}