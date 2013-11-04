package edu.berkeley.cs.amplab.adam.util

import edu.berkeley.cs.amplab.adam.commands._
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import spark.{RDD, SparkContext}
import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

object CommandCompanion extends AdamCommandCompanion {
  val commandName: String = "test"
  val commandDescription: String = "test"

  def apply(cmdLine: Array[String]): AdamCommand = new Command()
}

class Command extends AdamSparkCommand[SparkArgs] {
  protected val args: SparkArgs = new SparkArgs{spark_master = "local[4]"}

  def run(sc: SparkContext, job: Job): Unit = {
    val picardFile: RDD[ADAMRecord] = sc.adamLoad("/workspace/bams/test/PICARD.adam")
    val adamFile: RDD[ADAMRecord] = sc.adamLoad("/workspace/bams/test/ADAM.adam")

    /*
    val picardSorted = picardFile.adamSortReadsByReferencePosition()
    val adamSorted = adamFile.adamSortReadsByReferencePosition()

    val both = picardSorted.zip(adamSorted).coalesce(1)
    for ((picard, adam) <- both) {
      if (!picard.equals(adam)) {
        println("PICARD: " + picard)
        println("ADAM:" + adam)
      }
    }*/

    /*
    def f(p: ADAMRecord) =  {p.getDuplicateRead && p.getReadPaired && p.getReadMapped && p.getMateMapped}

    val a = picardFile.filter(f).adamSortReadsByReferencePosition()
    val b = adamFile.filter(f).adamSortReadsByReferencePosition()*/


    for (file <- List(picardFile, adamFile)) {
      println(file.filter(p => p.getDuplicateRead && p.getReadMapped && !p.getMateMapped && (p.getReferenceId == p.getMateReferenceId)).count())
    }

  }

  val companion: AdamCommandCompanion = CommandCompanion
}
