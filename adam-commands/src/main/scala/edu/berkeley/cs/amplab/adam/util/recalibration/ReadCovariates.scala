package edu.berkeley.cs.amplab.adam.util.recalibration

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import spark.broadcast.{Broadcast => SparkBroadcast}

/**
 *
 */
object ReadCovariates {
  def apply(rec: ADAMRecord, qualRG: QualByRG, covars: List[StandardCovariate], dbsnp: SparkBroadcast[Map[String,Set[Int]]] = null) : ReadCovariates = {
    val rich = RichADAMRecord(rec)
    val mask = rich.referencePositions.map(p => {
      p.isEmpty || dbsnp == null || dbsnp.value.contains(rec.getReferenceName.asInstanceOf[String]) &&
                                    dbsnp.value(rec.getReferenceName.toString).contains(p.get.toInt)
    }).toArray
    new ReadCovariates(rich,qualRG, covars, mask)
  }
}

class ReadCovariates(val read: RichADAMRecord, qualByRG: QualByRG, covars: List[StandardCovariate],
                     val dbsnpMask : Array[Boolean]) extends Iterator[BaseCovariates] with Serializable {

  val startOffset = read.qualityScores.takeWhile(_ <= 2).size
  val endOffset = read.qualityScores.size - read.qualityScores.reverseIterator.takeWhile(_ <= 2).size
  val qualCovar : Array[Int] = qualByRG(read,startOffset,endOffset)
  val requestedCovars : List[Array[Int]] = covars.map(covar => covar(read,startOffset,endOffset))

  var iter_position = startOffset

  override def hasNext : Boolean = iter_position < endOffset

  override def next() : BaseCovariates = {
    val idx = (iter_position-startOffset).toInt
    val position = read.getPosition(idx)
    val isMasked = dbsnpMask(idx) || read.isMismatchBase(idx).isEmpty
    val isMisMatch = read.isMismatchBase(idx).getOrElse(false) // getOrElse because reads without an MD tag can appear during *application* of recal table
    iter_position += 1
    new BaseCovariates(qualCovar(idx),requestedCovars.map(v => v(idx)).toArray,
                 read.qualityScores(idx),isMisMatch,isMasked)
  }

}

class BaseCovariates(val qualByRG: Int, val covar: Array[Int], val qual: Byte, val isMismatch: Boolean, val isMasked: Boolean) {} // holder class
