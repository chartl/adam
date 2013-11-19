package edu.berkeley.cs.amplab.adam.util.recalibration

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import scala.collection.JavaConversions._

object RecalUtil extends Serializable {

  object Constants {
    val MAX_REASONABLE_QSCORE = 60
    val MIN_REASONABLE_ERROR = math.pow(10.0,-MAX_REASONABLE_QSCORE/10)
    val MAX_NUMBER_OF_OBSERVATIONS = Int.MaxValue
  }

  def qualToErrorProb(q: Byte) : Double = math.pow(10.0,-q/10)
  def errorProbToQual(d: Double) : Byte = (-10 * math.log10(d)).toInt.toByte

  def warn(msg : String) {
    // this should be a logger, but currently the logger we're using doesn't work on the cluster
    // (defaults to a NoOp logger, which is not so useful)
    println("WARNING: "+msg)
  }

  def recalibrate(read: ADAMRecord, qualByRG: QualByRG, covars: List[StandardCovariate], table: RecalTable) : ADAMRecord = {
    var warned = false

    // get the covariates
    val readCovariates = ReadCovariates(read,qualByRG,covars)

    // it is possible for an entire read group not to contain MD tags. If this is the case
    // then no read will have been observed during table creation, and that read group
    // cannot be recalibrated.
    if ( qualByRG.getRGIndex(read).isEmpty || ! table.readGroupCounts.contains(qualByRG.getRGIndex(read).get) ) {
      if ( ! warned ) {
        warn("Read group "+read.getRecordGroupId+" was not observed during table creation")
      }
      return read
    }

    val toQual = errorProbToQual _
    val toErr = qualToErrorProb _
    val newQuals = readCovariates.map( b => {
      toQual(table.getErrorRateShifts(b).foldLeft(toErr(b.qual))(_+_))
    }).toArray
    val builder = ADAMRecord.newBuilder(read)
    builder.setQual(newQuals.foldLeft("")((a,b) => a + (b+33).toChar.toString))
    builder.build()
  }

}