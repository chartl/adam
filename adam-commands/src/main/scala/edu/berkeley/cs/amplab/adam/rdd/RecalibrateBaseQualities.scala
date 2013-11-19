package edu.berkeley.cs.amplab.adam.rdd

import spark.{RDD, Logging}
import spark.broadcast.{Broadcast => SparkBroadcast}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.util.recalibration._
import scala.collection.JavaConversions._

private[rdd] object RecalibrateBaseQualities extends Serializable with Logging {

  def usableRead(read: ADAMRecord) : Boolean = {
    // todo -- the mismatchingPositions should not merely be a filter, it should result in an exception. These are required for calculating mismatches.
    read.getReadMapped && read.getPrimaryAlignment && ! read.getDuplicateRead && (read.getMismatchingPositions != null)
  }

  def apply(rdd: RDD[ADAMRecord], dbsnp: SparkBroadcast[Map[String,Set[Int]]]) : RDD[ADAMRecord] = {
    // initialize the covariates
    log.info("Instantiating covariates...")
    println("Instantiating covariates...")
    // wtf this takes forever. Just put them on the command line.
    val readGroups = rdd.map(t => t.getRecordGroupId).distinct().collect().map(_.toString).toSeq
    //val readGroups = List("H06HD.1","H06HD.2","H06JU.1")
    val qualByRG = new QualByRG(readGroups)
    val otherCovars = List(new DiscreteCycle(),new BaseContext())
    log.info("Creating object...")
    println("Creating object...")
    val recalibrator = new RecalibrateBaseQualities(qualByRG,otherCovars)
    log.info("Computing table...")
    println("Computing table...")
    val table = recalibrator.computeTable(rdd,dbsnp)
    log.info("Finalizing table...")
    println("Finalizing table...")
    table.finalizeTable()
    log.info("Broadcasting table...")
    println("Broadcasting table...")
    val broadcastedTable = rdd.context.broadcast(table)
    //val dbsnp = null
    log.info("Applying table...")
    println("Applying table...")
    recalibrator.applyTable(broadcastedTable,rdd,qualByRG,otherCovars)
  }
}

private[rdd] class RecalibrateBaseQualities(val qualCovar: QualByRG, val covars: List[StandardCovariate]) extends Serializable with Logging {
  initLogging()

  def computeTable(rdd: RDD[ADAMRecord], dbsnp: SparkBroadcast[Map[String,Set[Int]]]) : RecalTable = {

    def usableRead(read: ADAMRecord) : Boolean = {
      // todo -- the mismatchingPositions should not merely be a filter, it should result in an exception. These are required for calculating mismatches.
      read.getReadMapped && read.getPrimaryAlignment && ! read.getDuplicateRead && (read.getMismatchingPositions != null)
      }

    def addCovariates(table: RecalTable, covar: ReadCovariates) : RecalTable = {
      //log.info("Aggregating covarates for read "+covar.read.record.getReadName.toString)
      if ( covar != null ) table + covar else table
    }

    def mergeTables(table1: RecalTable, table2: RecalTable) : RecalTable = {
      //log.info("Merging tables...")
      //println("Merging tables...")
      table1 ++ table2
    }

    rdd.map(r => if ( usableRead(r) ) ReadCovariates(r,qualCovar,covars,dbsnp) else null ).aggregate(new RecalTable)(addCovariates,mergeTables)
  }

  def applyTable(table: SparkBroadcast[RecalTable], rdd: RDD[ADAMRecord],qualCovar: QualByRG, covars: List[StandardCovariate]) : RDD[ADAMRecord] = {
    def recalibrate(record: ADAMRecord) : ADAMRecord = {
      if ( ! record.getReadMapped || ! record.getPrimaryAlignment || record.getDuplicateRead ) {
        record // no need to recalibrate these records todo -- enable optional recalibration of all reads
      } else {
        RecalUtil.recalibrate(record,qualCovar,covars,table.value)
      }
    }
    rdd.map(recalibrate)
  }
}
