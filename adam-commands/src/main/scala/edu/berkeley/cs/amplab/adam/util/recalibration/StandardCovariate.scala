package edu.berkeley.cs.amplab.adam.util.recalibration

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import spark.RDD
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord

/**
 *
 */

// this class is required, not just standard. Baked in to recalibration.
class QualByRG(groups: Seq[String]) extends Serializable {
  val readGroups = groups.zipWithIndex.toMap

  def apply(rich: RichADAMRecord, start: Int, end: Int) : Array[Int] = {
    val rg_offset = RecalUtil.Constants.MAX_REASONABLE_QSCORE*readGroups(rich.record.getRecordGroupId.toString)
    rich.qualityScores.slice(start,end).map(_.toInt + rg_offset)
  }

  def getRGIndex(read: ADAMRecord) : Option[Int] = {
    if ( readGroups.contains(read.getRecordGroupId.asInstanceOf[String]) ) {
      Some(readGroups(read.getRecordGroupId.asInstanceOf[String]))
    } else
      None
  }

  def numPartitions = RecalUtil.Constants.MAX_REASONABLE_QSCORE*(1+readGroups.size)
}

trait StandardCovariate extends Serializable {
  def apply(rec: RichADAMRecord, start: Int, end: Int) : Array[Int] // get the covariate for all the bases of the read
}

case class DiscreteCycle(noOp: Boolean = false) extends StandardCovariate {
  // this is a special-case of the GATK's Cycle covariate for discrete technologies.
  // Not to be used for 454 or ion torrent (which are flow cycles)
  override def apply(rich: RichADAMRecord, startOffset: Int, endOffset: Int) : Array[Int] = {
    val rec = rich.record
    var cycles : Array[Int] = if ( rec.getReadNegativeStrand ) Range(rec.getSequence.toString.size,0,-1).toArray
                              else Range(1,1+rec.getSequence.toString.size,1).toArray
    cycles = if ( rec.getReadPaired && rec.getSecondOfPair ) cycles.map(-_) else cycles
    cycles.slice(startOffset,endOffset)
  }
}

case class BaseContext(size: Int = 2) extends StandardCovariate {

  val BASES = Array('A'.toByte,'C'.toByte,'G'.toByte,'T'.toByte)
  val COMPL = Array('T'.toByte,'G'.toByte,'C'.toByte,'A'.toByte)
  val N_BASE = 'N'.toByte
  val COMPL_MP = ( BASES zip COMPL toMap ) + ( N_BASE -> N_BASE )

  override def apply(rich: RichADAMRecord, startOffset: Int, endOffset: Int) : Array[Int] = {
    val rec = rich.record
    // the context of a covariate is the previous @size bases, though "previous" depends on
    // how the read was aligned (negative strand is reverse-complemented).
    if ( rec.getReadNegativeStrand ) reverseContext(rec,startOffset,endOffset) else forwardContext(rec,startOffset,endOffset)
  }

  // note: the last base is dropped from the construction of contexts because it is not
  // present in any context - just as the first base cannot have a context assigned to it.
  def forwardContext(rec:ADAMRecord,st: Int, end: Int) : Array[Int] = {
    getContext(rec.getSequence.asInstanceOf[String].toCharArray.map(_.toByte).slice(st,end))
  }

  def simpleReverseComplement(bases: Array[Byte]) : Array[Byte] = {
    bases.map(b => COMPL_MP(b)).reverseIterator.toArray
  }

  def reverseContext(rec: ADAMRecord, st: Int, end: Int ) : Array[Int] = {
    // first reverse-complement the sequence
    val baseSeq = simpleReverseComplement(rec.getSequence.asInstanceOf[String].toCharArray.map(_.toByte))
    getContext(baseSeq.slice(baseSeq.size-end,baseSeq.size-st))
  }

  // the first @size bases don't get a context
  def getContext(bases : Array[Byte]) : Array[Int] = {
    (1 to size-1).map(_ => 0).toArray ++ bases.sliding(size).map(encode)
  }

  def encode(bases : Array[Byte]) : Int = {
    // takes a base sequence and maps it to an integer between 1 and 4^n+1
    // (0 is reserved for contextless bases (e.g. beginning of read)
    if ( bases.indexOf(N_BASE) > -1 ) 0 else 1 + bases.map(BASES.indexOf(_)).reduceLeft(_*4 + _)
  }

  def decode(bases : Int) : String = {
    if ( bases == 0 ) {
      "N"*size
    } else {
      var baseStr = ""
      var bInt = bases - 1
      while ( baseStr.size < size ) {
        val base = BASES(bInt % 4).toChar.toString
        baseStr = base + baseStr
        bInt /= 4
      }
      baseStr
    }
  }
}
