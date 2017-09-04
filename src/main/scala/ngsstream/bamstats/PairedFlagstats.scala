package ngsstream.bamstats

import ngsstream.utils.SamRecordPair

case class PairedFlagstats(r1: Flagstats,
                           r2: Flagstats,
                           secondary: Flagstats,
                           pairedFlagstats: Map[PairedFlagstats.PairedFlags.Value, Long]) {
  def +(other: PairedFlagstats): PairedFlagstats = {
    val pairedStats = PairedFlagstats.PairedFlags.values
      .map(flag => flag -> (other.pairedFlagstats.getOrElse(flag, 0L) +
        this.pairedFlagstats.getOrElse(flag, 0L))).toMap

    PairedFlagstats(other.r1 + this.r1, other.r2 + this.r2, other.secondary + this.secondary, pairedStats)
  }

  def primaryFlatstats: Flagstats = r1 + r2
  def totalFlatstats: Flagstats = primaryFlatstats + secondary
}

object PairedFlagstats {
  object PairedFlags extends Enumeration {
    val FF, FR, RF, RR, multiContig = Value
  }

  def generate(pairs: Iterator[SamRecordPair]): PairedFlagstats = {
    val list = pairs.toList
    PairedFlagstats(
      Flagstats.generate(list.toIterator.map(_.r1)),
      Flagstats.generate(list.toIterator.map(_.r2)),
      Flagstats.generate(list.toIterator.flatMap(_.secondary)),
      Map()
    )
  }
}
