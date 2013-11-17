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

  def recalibrate(read: ADAMRecord, qualByRG: QualByRG, covars: List[StandardCovariate], table: RecalTable) : ADAMRecord = {
    // get the covariates
    val readCovariates = ReadCovariates(read,qualByRG,covars)
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