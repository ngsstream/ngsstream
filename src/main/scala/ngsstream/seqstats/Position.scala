package ngsstream.seqstats

import ngsstream.utils.{Counts, Histogram}

case class Position(qual: Histogram[Byte] = new Histogram[Byte](),
                    nuc: Counts[Byte] = new Counts[Byte]()) {
  def +=(other: Position): Position = {
    this.qual += other.qual
    this.nuc += other.nuc
    this
  }
}
