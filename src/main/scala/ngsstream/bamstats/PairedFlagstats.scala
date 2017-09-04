package ngsstream.bamstats

import ngsstream.utils.{PairOrientation, SamRecordPair}

case class PairedFlagstats(r1: Flagstats,
                           r2: Flagstats,
                           secondary: Flagstats,
                           orientation: Map[PairOrientation.Value, Long]) {
  def +(other: PairedFlagstats): PairedFlagstats = {
    val oriMap = PairOrientation.values
      .map(flag => flag -> (other.orientation.getOrElse(flag, 0L) +
        this.orientation.getOrElse(flag, 0L))).toMap

    PairedFlagstats(other.r1 + this.r1, other.r2 + this.r2, other.secondary + this.secondary, oriMap)
  }

  def primaryFlatstats: Flagstats = r1 + r2
  def totalFlatstats: Flagstats = primaryFlatstats + secondary
}

object PairedFlagstats {
  def generate(pairs: Iterable[SamRecordPair]): PairedFlagstats = {
    PairedFlagstats(
      Flagstats.generate(pairs.map(_.r1)),
      Flagstats.generate(pairs.map(_.r2)),
      Flagstats.generate(pairs.flatMap(_.secondary)),
      pairs.groupBy(_.orientation).map(x => x._1 -> x._2.size.toLong)
    )
  }
}
